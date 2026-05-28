use axum::extract::rejection::JsonRejection;

use super::*;

const DEFAULT_IDENTITY_BATCH_LIMIT: usize = 1000;
const DEFAULT_IDENTITY_PAGE_SIZE: u64 = 100;

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub(super) struct ReverseIdentityRequestKey {
    pub(super) address: String,
    pub(super) coin_type: u64,
    pub(super) roles: IdentityRoles,
    pub(super) page_size: u64,
    pub(super) page_cursor: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub(super) struct ReverseIdentityStorageKey {
    address: String,
    coin_type: u64,
    roles: IdentityRoles,
    page_size: u64,
    page_cursor: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct IdentityNameLookup {
    pub(super) logical_name_id: String,
    pub(super) namespace: String,
    pub(super) normalized_name: String,
    pub(super) corrected_input_normalization: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum IdentityLookupProfile {
    Feed,
    Detail,
    Shadow,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(super) enum IdentityRoles {
    Owned,
    Managed,
    Both,
}

impl From<&ReverseIdentityRequestKey> for ReverseIdentityStorageKey {
    fn from(value: &ReverseIdentityRequestKey) -> Self {
        Self {
            address: value.address.clone(),
            coin_type: value.coin_type,
            roles: value.roles,
            page_size: value.page_size,
            page_cursor: value.page_cursor.clone(),
        }
    }
}

pub(super) fn parse_identity_json_body<T>(
    body: std::result::Result<Json<T>, JsonRejection>,
) -> ApiResult<T> {
    body.map(|Json(body)| body).map_err(|rejection| ApiError {
        status: StatusCode::BAD_REQUEST,
        code: "invalid_input",
        message: format!("malformed JSON request body: {rejection}"),
    })
}

pub(super) fn deduped_reverse_storage_inputs(
    requests: &[ReverseIdentityRequestKey],
) -> ApiResult<Vec<bigname_storage::ReverseIdentityStorageInput>> {
    requests
        .iter()
        .map(ReverseIdentityStorageKey::from)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .map(|request| {
            let pagination = PaginationRequest {
                active: request.page_cursor.is_some()
                    || request.page_size != DEFAULT_IDENTITY_PAGE_SIZE,
                cursor: request.page_cursor.clone(),
                page_size: request.page_size,
            };
            let cursor_spec =
                reverse_identity_cursor_spec(&request.address, request.coin_type, request.roles);
            reverse_storage_input(
                &request.address,
                request.coin_type,
                request.roles,
                &pagination,
                &cursor_spec,
            )
        })
        .collect()
}

pub(super) fn reverse_identity_group_key(
    group: &bigname_storage::ReverseIdentityGroup,
) -> ReverseIdentityStorageKey {
    let coin_type = group.input.coin_type.parse::<u64>().unwrap_or_default();
    let roles = IdentityRoles::from_storage(group.input.roles);
    ReverseIdentityStorageKey {
        address: group.input.address.clone(),
        coin_type,
        roles,
        page_size: group.input.page_size as u64,
        page_cursor: group.input.cursor.as_ref().map(|cursor| {
            let cursor_spec = reverse_identity_cursor_spec(&group.input.address, coin_type, roles);
            encode_cursor(&cursor_spec.envelope(reverse_identity_storage_cursor_item(cursor)))
        }),
    }
}

pub(super) fn reverse_identity_sort(
    left: &bigname_storage::ReverseIdentityRecordRow,
    right: &bigname_storage::ReverseIdentityRecordRow,
) -> std::cmp::Ordering {
    (
        !reverse_identity_is_primary(left),
        reverse_identity_role_rank(left),
        &left.name_record.row.normalized_name,
        &left.name_record.row.namespace,
        &left.name_record.row.namehash,
    )
        .cmp(&(
            !reverse_identity_is_primary(right),
            reverse_identity_role_rank(right),
            &right.name_record.row.normalized_name,
            &right.name_record.row.namespace,
            &right.name_record.row.namehash,
        ))
}

pub(super) fn reverse_identity_is_primary(
    record: &bigname_storage::ReverseIdentityRecordRow,
) -> bool {
    record.primary_name.as_ref().is_some_and(|primary| {
        primary.claim_status == bigname_storage::PrimaryNameClaimStatus::Success
            && primary.normalized_claim_name.as_deref()
                == Some(record.name_record.row.normalized_name.as_str())
    })
}

pub(super) fn reverse_identity_role_rank(
    record: &bigname_storage::ReverseIdentityRecordRow,
) -> u8 {
    if record.relation_facets.iter().any(|relation| {
        matches!(
            relation,
            bigname_storage::AddressNameRelation::TokenHolder
                | bigname_storage::AddressNameRelation::Registrant
        )
    }) {
        0
    } else {
        1
    }
}

pub(super) fn reverse_identity_cursor_spec(
    address: &str,
    coin_type: u64,
    roles: IdentityRoles,
) -> CursorSpec {
    let mut filters = BTreeMap::new();
    filters.insert("coin_type".to_owned(), coin_type.to_string());
    filters.insert("roles".to_owned(), roles.as_str().to_owned());
    CursorSpec {
        route: "/v1/identity:lookup",
        anchor: address.to_owned(),
        sort: "primary_role_name_namespace_namehash_asc",
        filters,
    }
}

pub(super) fn reverse_identity_cursor_item(
    record: &bigname_storage::ReverseIdentityRecordRow,
) -> BTreeMap<String, String> {
    let mut item = BTreeMap::new();
    item.insert(
        "is_primary".to_owned(),
        reverse_identity_is_primary(record).to_string(),
    );
    item.insert(
        "role_rank".to_owned(),
        reverse_identity_role_rank(record).to_string(),
    );
    item.insert(
        "normalized_name".to_owned(),
        record.name_record.row.normalized_name.clone(),
    );
    item.insert(
        "namespace".to_owned(),
        record.name_record.row.namespace.clone(),
    );
    item.insert(
        "namehash".to_owned(),
        record.name_record.row.namehash.clone(),
    );
    item
}

pub(super) fn reverse_identity_storage_cursor_item(
    cursor: &bigname_storage::ReverseIdentityCursor,
) -> BTreeMap<String, String> {
    let mut item = BTreeMap::new();
    item.insert("is_primary".to_owned(), cursor.is_primary.to_string());
    item.insert("role_rank".to_owned(), cursor.role_rank.to_string());
    item.insert("normalized_name".to_owned(), cursor.normalized_name.clone());
    item.insert("namespace".to_owned(), cursor.namespace.clone());
    item.insert("namehash".to_owned(), cursor.namehash.clone());
    item
}

pub(super) fn parse_identity_lookup_profile(
    value: Option<&str>,
) -> ApiResult<IdentityLookupProfile> {
    match value.unwrap_or("detail").trim().to_ascii_lowercase().as_str() {
        "feed" => Ok(IdentityLookupProfile::Feed),
        "detail" => Ok(IdentityLookupProfile::Detail),
        "shadow" => Ok(IdentityLookupProfile::Shadow),
        _ => Err(ApiError {
            status: StatusCode::BAD_REQUEST,
            code: "invalid_input",
            message: "profile must be one of: feed, detail, shadow".to_owned(),
        }),
    }
}

pub(super) fn parse_identity_lookup_namespace(value: Option<&str>) -> ApiResult<Option<&str>> {
    match value.map(str::trim).filter(|value| !value.is_empty()) {
        None | Some("auto") | Some("public") => Ok(None),
        Some("ens") => Ok(Some("ens")),
        Some("basenames") => Ok(Some("basenames")),
        _ => Err(ApiError {
            status: StatusCode::BAD_REQUEST,
            code: "invalid_input",
            message: "namespace must be one of: auto, public, ens, basenames".to_owned(),
        }),
    }
}

pub(super) fn parse_identity_lookup_roles(values: Option<&[String]>) -> ApiResult<IdentityRoles> {
    let Some(values) = values else {
        return Ok(IdentityRoles::Both);
    };
    if values.is_empty() {
        return Ok(IdentityRoles::Both);
    }

    let mut owned = false;
    let mut managed = false;
    for value in values {
        match value.trim().to_ascii_lowercase().as_str() {
            "owned" | "registrant" | "token_holder" => owned = true,
            "managed" | "effective_controller" => managed = true,
            "both" | "any" => {
                owned = true;
                managed = true;
            }
            _ => {
                return Err(ApiError {
                    status: StatusCode::BAD_REQUEST,
                    code: "invalid_input",
                    message:
                        "roles must contain only owned, managed, registrant, effective_controller, both, or any"
                            .to_owned(),
                });
            }
        }
    }

    match (owned, managed) {
        (true, true) => Ok(IdentityRoles::Both),
        (true, false) => Ok(IdentityRoles::Owned),
        (false, true) => Ok(IdentityRoles::Managed),
        (false, false) => Ok(IdentityRoles::Both),
    }
}

pub(super) fn native_identity_roles(values: IdentityRoles) -> Vec<String> {
    match values {
        IdentityRoles::Owned => vec!["owned".to_owned()],
        IdentityRoles::Managed => vec!["managed".to_owned()],
        IdentityRoles::Both => vec!["owned".to_owned(), "managed".to_owned()],
    }
}

pub(super) fn parse_identity_pagination_with_default(
    cursor: Option<&str>,
    page_size: Option<u64>,
    default_page_size: u64,
) -> ApiResult<PaginationRequest> {
    let cursor = cursor
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned);
    let active = cursor.is_some() || page_size.is_some();
    let page_size = match page_size {
        None => default_page_size,
        Some(value) if !(1..=MAX_PAGE_SIZE).contains(&value) => {
            return Err(ApiError {
                status: StatusCode::BAD_REQUEST,
                code: "invalid_input",
                message: format!("page_size must be between 1 and {MAX_PAGE_SIZE}"),
            });
        }
        Some(value) => value,
    };

    Ok(PaginationRequest {
        active,
        cursor,
        page_size,
    })
}

pub(super) fn reverse_storage_input(
    address: &str,
    coin_type: u64,
    roles: IdentityRoles,
    pagination: &PaginationRequest,
    cursor_spec: &CursorSpec,
) -> ApiResult<bigname_storage::ReverseIdentityStorageInput> {
    Ok(bigname_storage::ReverseIdentityStorageInput {
        address: address.to_owned(),
        coin_type: coin_type.to_string(),
        roles: roles.storage_roles(),
        page_size: pagination.page_size as i64,
        cursor: reverse_identity_storage_cursor(pagination, cursor_spec)?,
    })
}

pub(super) fn reverse_identity_storage_cursor(
    pagination: &PaginationRequest,
    cursor_spec: &CursorSpec,
) -> ApiResult<Option<bigname_storage::ReverseIdentityCursor>> {
    let Some(item) = decoded_cursor_item(pagination, cursor_spec)? else {
        return Ok(None);
    };
    require_cursor_item_fields(
        &item,
        &[
            "is_primary",
            "role_rank",
            "normalized_name",
            "namespace",
            "namehash",
        ],
    )?;

    let is_primary = required_cursor_item_field(&item, "is_primary")?
        .parse::<bool>()
        .map_err(|_| invalid_cursor_error())?;
    let role_rank = required_cursor_item_field(&item, "role_rank")?
        .parse::<i16>()
        .map_err(|_| invalid_cursor_error())?;

    Ok(Some(bigname_storage::ReverseIdentityCursor {
        is_primary,
        role_rank,
        normalized_name: required_cursor_item_field(&item, "normalized_name")?.to_owned(),
        namespace: required_cursor_item_field(&item, "namespace")?.to_owned(),
        namehash: required_cursor_item_field(&item, "namehash")?.to_owned(),
    }))
}

pub(super) fn parse_identity_name_lookup_with_namespace(
    name: &str,
    namespace: Option<&str>,
) -> Result<IdentityNameLookup, RouteNameNormalizationError> {
    let parsed = normalize_inferred_route_name(name)?;
    let namespace = namespace.unwrap_or(parsed.namespace).to_owned();
    Ok(IdentityNameLookup {
        logical_name_id: format!("{}:{}", namespace, parsed.normalized_name),
        namespace,
        normalized_name: parsed.normalized_name,
        corrected_input_normalization: parsed.corrected_input_normalization,
    })
}

pub(super) fn ensure_identity_batch_limit(input_count: usize) -> ApiResult<()> {
    let limit = identity_batch_limit();
    if input_count > limit {
        return Err(ApiError {
            status: StatusCode::BAD_REQUEST,
            code: "invalid_input",
            message: format!("identity batch size must not exceed {limit} inputs"),
        });
    }
    Ok(())
}

fn identity_batch_limit() -> usize {
    std::env::var("BIGNAME_API_IDENTITY_BATCH_LIMIT")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_IDENTITY_BATCH_LIMIT)
}

impl IdentityRoles {
    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::Owned => "OWNED",
            Self::Managed => "MANAGED",
            Self::Both => "BOTH",
        }
    }

    pub(super) fn storage_roles(self) -> bigname_storage::ReverseIdentityRoles {
        match self {
            Self::Owned => bigname_storage::ReverseIdentityRoles::Owned,
            Self::Managed => bigname_storage::ReverseIdentityRoles::Managed,
            Self::Both => bigname_storage::ReverseIdentityRoles::Both,
        }
    }

    pub(super) fn from_storage(value: bigname_storage::ReverseIdentityRoles) -> Self {
        match value {
            bigname_storage::ReverseIdentityRoles::Owned => Self::Owned,
            bigname_storage::ReverseIdentityRoles::Managed => Self::Managed,
            bigname_storage::ReverseIdentityRoles::Both => Self::Both,
        }
    }
}
