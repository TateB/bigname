use axum::{Json, extract::rejection::JsonRejection};

use super::{
    cursor::{LookupReverseCursorBinding, lookup_reverse_storage_cursor},
    dto::{
        LookupAddressInput, LookupNameInput, LookupRequest, LookupResultInput, NormalizationInfo,
    },
};
use crate::{
    normalize_inferred_route_name, parse_evm_address,
    v2::{
        DEFAULT_PAGE_SIZE, MAX_PAGE_SIZE, Relation, RelationSet, V2Error, V2Result,
        api_error_to_v2, decode, encode, parse_relation_set_param,
    },
};

const DEFAULT_LOOKUP_BATCH_LIMIT: usize = 1000;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct ParsedNameLookup {
    pub(super) index: usize,
    pub(super) input: LookupResultInput,
    pub(super) lookup: Option<IdentityNameLookup>,
    pub(super) normalization: Option<NormalizationInfo>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct ParsedAddressLookup {
    pub(super) index: usize,
    pub(super) input: LookupResultInput,
    pub(super) address: String,
    pub(super) coin_type: u64,
    pub(super) relation: Option<RelationSet>,
    pub(super) roles: bigname_storage::ReverseIdentityRoles,
    pub(super) page_size: u64,
    pub(super) page_cursor: Option<bigname_storage::ReverseIdentityCursor>,
    pub(super) page_cursor_token: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct IdentityNameLookup {
    pub(super) logical_name_id: String,
    corrected_input_normalization: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum LookupProfile {
    Feed,
    Detail,
}

pub(super) fn parse_name_input(
    index: usize,
    input: &LookupNameInput,
    namespace: Option<&str>,
) -> V2Result<ParsedNameLookup> {
    let id = optional_id(input.id.as_deref())?;
    let input_name = input.name.clone();
    let (lookup, normalization) =
        match parse_identity_name_lookup_with_namespace(&input_name, namespace) {
            Ok(lookup) => {
                let normalization =
                    lookup
                        .corrected_input_normalization
                        .then(|| NormalizationInfo {
                            changed: true,
                            input_name: input_name.clone(),
                            reason: "case_normalized".to_owned(),
                        });
                (Some(lookup), normalization)
            }
            Err(_) => (
                None,
                Some(NormalizationInfo {
                    changed: false,
                    input_name: input_name.clone(),
                    reason: "invalid_normalized_name".to_owned(),
                }),
            ),
        };

    Ok(ParsedNameLookup {
        index,
        input: LookupResultInput {
            id,
            name: Some(input_name),
            address: None,
            coin_type: None,
            relation: None,
            page_size: None,
            cursor: None,
        },
        lookup,
        normalization,
    })
}

pub(super) fn parse_address_input(
    index: usize,
    input: &LookupAddressInput,
) -> V2Result<ParsedAddressLookup> {
    let id = optional_id(input.id.as_deref())?;
    let address = parse_evm_address(&input.address, "address").map_err(api_error_to_v2)?;
    let coin_type = input.coin_type.unwrap_or(60);
    let coin_type = crate::parse_primary_name_coin_type(Some(&coin_type.to_string()))
        .map_err(api_error_to_v2)?
        .parse::<u64>()
        .map_err(|_| V2Error::invalid_input("coin_type must fit in an unsigned 64-bit integer"))?;
    let relation = parse_relation_set_param(input.relation.as_deref())?;
    let roles = relation_to_storage_roles(relation.as_ref());
    let page_size = parse_page_size(input.page_size)?;
    let (page_cursor, page_cursor_token) = parse_reverse_cursor(
        input.cursor.as_deref(),
        &address,
        coin_type,
        relation.as_ref(),
    )?;

    Ok(ParsedAddressLookup {
        index,
        input: LookupResultInput {
            id,
            name: None,
            address: Some(address.clone()),
            coin_type: Some(coin_type),
            relation: relation.as_ref().map(RelationSet::canonical_value),
            page_size: input.page_size,
            cursor: input.cursor.clone(),
        },
        address,
        coin_type,
        relation,
        roles,
        page_size,
        page_cursor,
        page_cursor_token,
    })
}

pub(super) fn parse_lookup_json_body(
    body: Result<Json<LookupRequest>, JsonRejection>,
) -> V2Result<LookupRequest> {
    body.map(|Json(body)| body).map_err(|rejection| {
        V2Error::invalid_input(format!("malformed JSON request body: {rejection}"))
    })
}

pub(super) fn parse_lookup_profile(value: Option<&str>) -> V2Result<LookupProfile> {
    match value
        .unwrap_or("detail")
        .trim()
        .to_ascii_lowercase()
        .as_str()
    {
        "feed" => Ok(LookupProfile::Feed),
        "detail" | "shadow" => Ok(LookupProfile::Detail),
        _ => Err(V2Error::invalid_input(
            "profile must be one of: feed, detail",
        )),
    }
}

pub(super) fn parse_lookup_namespace(value: Option<&str>) -> V2Result<Option<&str>> {
    match value.map(str::trim).filter(|value| !value.is_empty()) {
        None | Some("auto") | Some("public") => Ok(None),
        Some("ens") => Ok(Some("ens")),
        Some("basenames") => Ok(Some("basenames")),
        _ => Err(V2Error::invalid_input(
            "namespace must be one of: auto, public, ens, basenames",
        )),
    }
}

pub(super) fn ensure_lookup_batch_limit(input_count: usize) -> V2Result<()> {
    let limit = lookup_batch_limit();
    if input_count > limit {
        return Err(V2Error::invalid_input(format!(
            "lookup batch size must not exceed {limit} inputs"
        )));
    }
    Ok(())
}

fn parse_reverse_cursor(
    cursor: Option<&str>,
    address: &str,
    coin_type: u64,
    relation: Option<&RelationSet>,
) -> V2Result<(
    Option<bigname_storage::ReverseIdentityCursor>,
    Option<String>,
)> {
    let Some(cursor) = cursor.map(str::trim).filter(|cursor| !cursor.is_empty()) else {
        return Ok((None, None));
    };
    let binding = LookupReverseCursorBinding {
        address,
        coin_type,
        relation,
    };
    let payload = decode(cursor)?;
    let storage_cursor = lookup_reverse_storage_cursor(&payload, &binding)?;
    Ok((Some(storage_cursor), Some(encode(&payload))))
}

fn relation_to_storage_roles(
    relation: Option<&RelationSet>,
) -> bigname_storage::ReverseIdentityRoles {
    match relation {
        Some(relation) if relation.is_exact_manager() => {
            bigname_storage::ReverseIdentityRoles::Managed
        }
        Some(relation)
            if relation
                .as_slice()
                .iter()
                .all(|relation| matches!(relation, Relation::Owner | Relation::Registrant)) =>
        {
            bigname_storage::ReverseIdentityRoles::Owned
        }
        None => bigname_storage::ReverseIdentityRoles::Both,
        Some(_) => bigname_storage::ReverseIdentityRoles::Both,
    }
}

fn parse_page_size(value: Option<u64>) -> V2Result<u64> {
    match value {
        None => Ok(DEFAULT_PAGE_SIZE),
        Some(value) if !(1..=MAX_PAGE_SIZE).contains(&value) => Err(V2Error::invalid_input(
            format!("page_size must be between 1 and {MAX_PAGE_SIZE}"),
        )),
        Some(value) => Ok(value),
    }
}

fn optional_id(value: Option<&str>) -> V2Result<Option<String>> {
    value
        .map(str::trim)
        .map(|value| {
            (!value.is_empty())
                .then(|| value.to_owned())
                .ok_or_else(|| V2Error::invalid_input("lookup input id must not be empty"))
        })
        .transpose()
}

fn parse_identity_name_lookup_with_namespace(
    name: &str,
    namespace: Option<&str>,
) -> Result<IdentityNameLookup, crate::RouteNameNormalizationError> {
    let parsed = normalize_inferred_route_name(name)?;
    let namespace = namespace.unwrap_or(parsed.namespace).to_owned();
    Ok(IdentityNameLookup {
        logical_name_id: format!("{}:{}", namespace, parsed.normalized_name),
        corrected_input_normalization: parsed.corrected_input_normalization,
    })
}

fn lookup_batch_limit() -> usize {
    std::env::var("BIGNAME_API_LOOKUP_BATCH_LIMIT")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_LOOKUP_BATCH_LIMIT)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::v2::ErrorCode;

    #[test]
    fn lookup_profile_shadow_is_hidden_detail_alias() {
        assert_eq!(
            parse_lookup_profile(Some("shadow")).expect("shadow alias must parse"),
            LookupProfile::Detail
        );
    }

    #[test]
    fn lookup_relation_maps_to_existing_storage_roles() {
        assert_eq!(
            relation_to_storage_roles(Some(&RelationSet::from(Relation::Owner))),
            bigname_storage::ReverseIdentityRoles::Owned
        );
        assert_eq!(
            relation_to_storage_roles(Some(&RelationSet::from(Relation::Registrant))),
            bigname_storage::ReverseIdentityRoles::Owned
        );
        assert_eq!(
            relation_to_storage_roles(Some(&RelationSet::from(Relation::Manager))),
            bigname_storage::ReverseIdentityRoles::Managed
        );
        assert_eq!(
            relation_to_storage_roles(None),
            bigname_storage::ReverseIdentityRoles::Both
        );
    }

    #[test]
    fn lookup_page_size_uses_v2_limit() {
        assert_eq!(
            parse_page_size(None).expect("default page size"),
            DEFAULT_PAGE_SIZE
        );
        assert_eq!(parse_page_size(Some(MAX_PAGE_SIZE)).unwrap(), MAX_PAGE_SIZE);
        let error = parse_page_size(Some(MAX_PAGE_SIZE + 1)).unwrap_err();
        assert_eq!(error.code(), ErrorCode::InvalidInput);
    }
}
