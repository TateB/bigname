use std::collections::{BTreeMap, BTreeSet};

use axum::{
    Json,
    extract::{State, rejection::JsonRejection},
};
use tracing::error;

use crate::AppState;

use super::{Envelope, Meta, NoQueryParams, Page, Relation, Status, V2Error, V2Result, encode};

mod build;
mod cursor;
mod dto;
mod head;
mod parse;
mod scope;

use build::{
    build_forward_detail_record, build_forward_feed_record, build_reverse_detail_record,
    build_reverse_feed_record, lookup_address_status,
};
use cursor::{
    LookupReverseCursorBinding, lookup_reverse_cursor_payload, reverse_identity_sort,
    reverse_identity_storage_cursor,
};
use dto::{LookupInput, LookupKind, LookupRecord, LookupRequest, LookupResult};
pub(crate) use head::load_served_head_meta;
use parse::{
    LookupProfile, ParsedAddressLookup, ParsedNameLookup, ensure_lookup_batch_limit,
    parse_address_input, parse_lookup_json_body, parse_lookup_namespace, parse_lookup_profile,
    parse_name_input,
};
use scope::lookup_snapshot_scope;

const EXACT_RELATION_SCAN_MULTIPLIER: u64 = 10;

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
    _query: NoQueryParams,
    State(state): State<AppState>,
    body: Result<Json<LookupRequest>, JsonRejection>,
) -> V2Result<Json<Envelope<Vec<LookupResult>>>> {
    let body = parse_lookup_json_body(body)?;
    ensure_lookup_batch_limit(body.inputs.len())?;
    let profile = parse_lookup_profile(body.profile.as_deref())?;
    let namespace = parse_lookup_namespace(body.namespace.as_deref())?;

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
                address_inputs.push(parse_address_input(index, input)?);
            }
        }
    }
    let snapshot_scope =
        lookup_snapshot_scope(&state, namespace, &name_inputs, !address_inputs.is_empty()).await?;
    let served_head = match snapshot_scope.as_ref() {
        Some(scope) => load_served_head_meta(&state.pool, scope).await?,
        None => Meta::default(),
    };

    let mut results = vec![None; body.inputs.len()];
    render_name_lookup_results(&state, profile, &name_inputs, &mut results).await?;
    render_reverse_lookup_results(&state, profile, &address_inputs, &mut results).await?;

    let data = results
        .into_iter()
        .map(|result| result.expect("every parsed lookup input must render a result"))
        .collect::<Vec<_>>();

    Ok(Json(Envelope {
        data,
        page: None,
        meta: served_head,
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
                    }?;
                    (record.status, Some(record))
                }
                None => (Status::NotFound, None),
            },
        };
        results[input.index] = Some(LookupResult {
            input: input.input.clone(),
            kind: LookupKind::Name,
            status,
            unsupported_reason: result_unsupported_reason(status, record.iter()),
            failure_reason: result_failure_reason(status, record.iter()),
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

async fn render_reverse_lookup_results(
    state: &AppState,
    profile: LookupProfile,
    inputs: &[ParsedAddressLookup],
    results: &mut [Option<LookupResult>],
) -> V2Result<()> {
    render_storage_exact_reverse_lookup_results(state, profile, inputs, results).await?;
    for input in inputs
        .iter()
        .filter(|input| requires_relation_post_filter(input.relation))
    {
        let page = load_exact_relation_reverse_page(state, input).await?;
        render_reverse_input_result(profile, input, page, results)?;
    }
    Ok(())
}

async fn render_storage_exact_reverse_lookup_results(
    state: &AppState,
    profile: LookupProfile,
    inputs: &[ParsedAddressLookup],
    results: &mut [Option<LookupResult>],
) -> V2Result<()> {
    let storage_exact_inputs = inputs
        .iter()
        .filter(|input| !requires_relation_post_filter(input.relation))
        .collect::<Vec<_>>();
    let storage_inputs = deduped_reverse_storage_inputs(storage_exact_inputs.iter().copied());
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

    for input in storage_exact_inputs {
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
        let page = ReverseLookupPage {
            entries,
            next_cursor,
            total_count,
            has_more,
        };
        render_reverse_input_result(profile, input, page, results)?;
    }

    Ok(())
}

async fn load_exact_relation_reverse_page(
    state: &AppState,
    input: &ParsedAddressLookup,
) -> V2Result<ReverseLookupPage> {
    let target_len = input.page_size as usize;
    let scan_size = input.page_size.max(50);
    let scan_cap = scan_size.saturating_mul(EXACT_RELATION_SCAN_MULTIPLIER);
    let mut cursor = input.page_cursor.clone();
    let mut entries = Vec::with_capacity(target_len.saturating_add(1));
    let mut has_more = false;
    let mut hit_scan_cap = false;
    let mut rows_examined = 0_u64;
    let mut last_examined = None;

    loop {
        let storage_input = bigname_storage::ReverseIdentityStorageInput {
            address: input.address.clone(),
            coin_type: input.coin_type.to_string(),
            roles: input.roles,
            page_size: scan_size as i64,
            cursor: cursor.clone(),
        };
        let mut groups = bigname_storage::load_reverse_identity_records(
            &state.pool,
            std::slice::from_ref(&storage_input),
        )
        .await
        .map_err(|load_error| {
            error!(
                service = "api",
                input_count = 1,
                relation = ?input.relation,
                error = ?load_error,
                "failed to load v2 lookup reverse exact-relation records"
            );
            V2Error::internal_error("failed to load lookup reverse records")
        })?;
        let Some(mut group) = groups.pop() else {
            break;
        };

        group.entries.sort_by(reverse_identity_sort);
        let broad_has_more = group.has_more;
        if group.entries.is_empty() {
            break;
        }
        for entry in group.entries {
            let next_scan_cursor = reverse_identity_storage_cursor(&entry);
            rows_examined = rows_examined.saturating_add(1);
            last_examined = Some(entry.clone());
            if reverse_record_matches_relation(&entry, input.relation) {
                entries.push(trim_reverse_record_relations(entry, input.relation));
                if entries.len() > target_len {
                    has_more = true;
                    break;
                }
            }
            cursor = Some(next_scan_cursor);
            if rows_examined >= scan_cap && broad_has_more {
                hit_scan_cap = true;
                break;
            }
        }

        if has_more || hit_scan_cap || !broad_has_more {
            break;
        }
        if cursor.is_none() {
            break;
        }
    }

    let binding = LookupReverseCursorBinding {
        address: &input.address,
        coin_type: input.coin_type,
        relation: input.relation,
    };
    let next_cursor_record = if has_more {
        entries.truncate(target_len);
        entries.last()
    } else if hit_scan_cap {
        has_more = true;
        last_examined.as_ref()
    } else {
        None
    };
    let next_cursor =
        next_cursor_record.map(|record| encode(&lookup_reverse_cursor_payload(record, &binding)));

    Ok(ReverseLookupPage {
        entries,
        next_cursor,
        total_count: None,
        has_more,
    })
}

fn render_reverse_input_result(
    profile: LookupProfile,
    input: &ParsedAddressLookup,
    page: ReverseLookupPage,
    results: &mut [Option<LookupResult>],
) -> V2Result<()> {
    let records = page
        .entries
        .iter()
        .map(|record| match profile {
            LookupProfile::Feed => build_reverse_feed_record(record),
            LookupProfile::Detail => build_reverse_detail_record(record),
        })
        .collect::<V2Result<Vec<_>>>()?;
    let status = lookup_address_status(&records);
    results[input.index] = Some(address_lookup_result(
        input,
        records,
        page.next_cursor,
        page.total_count,
        page.has_more,
        status,
    ));
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
        unsupported_reason: result_unsupported_reason(status, records.iter()),
        failure_reason: result_failure_reason(status, records.iter()),
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

#[derive(Clone, Debug, Eq, PartialEq)]
struct ReverseLookupPage {
    entries: Vec<bigname_storage::ReverseIdentityRecordRow>,
    next_cursor: Option<String>,
    total_count: Option<u64>,
    has_more: bool,
}

fn result_unsupported_reason<'a>(
    status: Status,
    records: impl Iterator<Item = &'a LookupRecord>,
) -> Option<String> {
    (status == Status::Unsupported)
        .then(|| {
            records
                .filter_map(|record| record.unsupported_reason.clone())
                .next()
        })
        .flatten()
}

fn result_failure_reason<'a>(
    status: Status,
    records: impl Iterator<Item = &'a LookupRecord>,
) -> Option<String> {
    matches!(status, Status::Failed | Status::NotFound | Status::Mismatch)
        .then(|| {
            records
                .filter_map(|record| record.failure_reason.clone())
                .next()
        })
        .flatten()
}

fn requires_relation_post_filter(relation: Option<Relation>) -> bool {
    matches!(relation, Some(Relation::Owner | Relation::Registrant))
}

fn reverse_record_matches_relation(
    record: &bigname_storage::ReverseIdentityRecordRow,
    relation: Option<Relation>,
) -> bool {
    relation.is_none_or(|relation| {
        record
            .relation_facets
            .contains(&relation_to_storage(relation))
    })
}

fn trim_reverse_record_relations(
    mut record: bigname_storage::ReverseIdentityRecordRow,
    relation: Option<Relation>,
) -> bigname_storage::ReverseIdentityRecordRow {
    if let Some(relation) = relation {
        let relation = relation_to_storage(relation);
        record.relation_facets.retain(|facet| *facet == relation);
    }
    record
}

fn relation_to_storage(relation: Relation) -> bigname_storage::AddressNameRelation {
    match relation {
        Relation::Owner => bigname_storage::AddressNameRelation::TokenHolder,
        Relation::Manager => bigname_storage::AddressNameRelation::EffectiveController,
        Relation::Registrant => bigname_storage::AddressNameRelation::Registrant,
    }
}

fn deduped_reverse_storage_inputs<'a>(
    inputs: impl Iterator<Item = &'a ParsedAddressLookup>,
) -> Vec<bigname_storage::ReverseIdentityStorageInput> {
    inputs
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
