#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct NamesInclude {
    total_count: bool,
    record_summaries: bool,
}

#[derive(Clone, Debug)]
struct ParsedNamesRequest {
    filter: bigname_storage::NameCurrentListFilter,
    sort: bigname_storage::NameCurrentListSort,
    order: bigname_storage::NameCurrentListOrder,
    include: NamesInclude,
    meta: MetaMode,
    pagination: PaginationRequest,
    unsupported_fields: Vec<String>,
}

impl ParsedNamesRequest {
    fn cursor_spec(&self) -> CursorSpec {
        let mut filters = BTreeMap::new();
        if let Some(namespace) = self.filter.namespace.as_ref() {
            filters.insert("namespace".to_owned(), namespace.clone());
        }
        if let Some(name) = self.filter.name.as_ref() {
            filters.insert("name".to_owned(), name.clone());
        }
        if let Some(prefix) = self.filter.prefix.as_ref() {
            filters.insert("prefix".to_owned(), prefix.clone());
        }
        if let Some(contains) = self.filter.contains.as_ref() {
            filters.insert("contains".to_owned(), contains.clone());
        }
        if let Some(contains_nocase) = self.filter.contains_nocase.as_ref() {
            filters.insert("contains_nocase".to_owned(), contains_nocase.clone());
        }
        if let Some(resolver) = self.filter.resolver.as_ref() {
            filters.insert("resolver".to_owned(), resolver.clone());
        }
        if let Some(address) = self.filter.address.as_ref() {
            filters.insert("address".to_owned(), address.address.clone());
            filters.insert("relation".to_owned(), address.relation.as_str().to_owned());
        }

        CursorSpec {
            route: "/v1/names",
            anchor: "names".to_owned(),
            sort: names_sort_label(self.sort, self.order),
            filters,
        }
    }
}

impl MetaMode {
    fn include_summary(self) -> bool {
        !matches!(self, Self::None)
    }
}

fn parse_names_request(query: NamesQuery) -> ApiResult<ParsedNamesRequest> {
    parse_compact_only_response_view(
        query.view.as_deref(),
        "view=full is reserved for a later compact names implementation",
    )?;
    let meta = parse_meta_mode(query.meta.as_deref(), MetaMode::Summary)?;
    if query
        .resolved_address
        .as_deref()
        .map(str::trim)
        .is_some_and(|value| !value.is_empty())
    {
        return Err(unsupported_filter_error(
            "resolved_address",
            "resolved_address filtering requires a declared record-value equality projection",
        ));
    }

    let address = parse_names_address_filter(&query)?;
    let namespace = parse_address_names_namespace(query.namespace.as_deref())?;
    let name = parse_optional_exact_name_query_value(query.name, "name")?;
    let prefix = parse_optional_nonempty_query_value(query.prefix, "prefix")?;
    let contains = parse_optional_nonempty_query_value(query.contains, "contains")?;
    let contains_nocase =
        parse_optional_nonempty_query_value(query.contains_nocase, "contains_nocase")?;
    let resolver = parse_optional_address_filter("resolver", query.resolver.as_deref())?;
    let sort = parse_names_sort(query.sort.as_deref())?;
    let order = parse_names_order(query.order.as_deref())?;
    let include = parse_names_include(query.include.as_deref())?;
    let pagination = parse_pagination(query.cursor.as_deref(), query.page_size)?;
    let unsupported_fields = include
        .record_summaries
        .then(|| "record_summaries".to_owned())
        .into_iter()
        .collect();

    Ok(ParsedNamesRequest {
        filter: bigname_storage::NameCurrentListFilter {
            namespace,
            name,
            prefix,
            contains,
            contains_nocase,
            resolver,
            address,
        },
        sort,
        order,
        include,
        meta,
        pagination,
        unsupported_fields,
    })
}

fn parse_names_address_filter(
    query: &NamesQuery,
) -> ApiResult<Option<bigname_storage::NameCurrentAddressFilter>> {
    let owner = parse_optional_address_filter("owner", query.owner.as_deref())?;
    let account = parse_optional_address_filter("account", query.account.as_deref())?;
    let registrant = parse_optional_address_filter("registrant", query.registrant.as_deref())?;
    let supplied_count = owner
        .iter()
        .chain(account.iter())
        .chain(registrant.iter())
        .count();
    if supplied_count > 1 {
        return Err(ApiError {
            status: StatusCode::BAD_REQUEST,
            code: "invalid_input",
            message: "only one of owner, account, or registrant may be supplied".to_owned(),
        });
    }

    let relation = parse_optional_app_relation(query.relation.as_deref())?;
    match (owner, account, registrant, relation) {
        (Some(owner), None, None, None)
        | (
            Some(owner),
            None,
            None,
            Some(bigname_storage::NameCurrentAddressRelationFilter::Relation(
                AddressNameRelation::TokenHolder,
            )),
        ) => Ok(Some(bigname_storage::NameCurrentAddressFilter {
            address: owner,
            relation: bigname_storage::NameCurrentAddressRelationFilter::Relation(
                AddressNameRelation::TokenHolder,
            ),
        })),
        (Some(_), None, None, Some(_)) => Err(ApiError {
            status: StatusCode::BAD_REQUEST,
            code: "invalid_input",
            message: "owner may only be paired with relation=token_holder".to_owned(),
        }),
        (None, None, Some(registrant), None)
        | (
            None,
            None,
            Some(registrant),
            Some(bigname_storage::NameCurrentAddressRelationFilter::Relation(
                AddressNameRelation::Registrant,
            )),
        ) => Ok(Some(bigname_storage::NameCurrentAddressFilter {
            address: registrant,
            relation: bigname_storage::NameCurrentAddressRelationFilter::Relation(
                AddressNameRelation::Registrant,
            ),
        })),
        (None, None, Some(_), Some(_)) => Err(ApiError {
            status: StatusCode::BAD_REQUEST,
            code: "invalid_input",
            message: "registrant may only be paired with relation=registrant".to_owned(),
        }),
        (None, Some(account), None, relation) => Ok(Some(
            bigname_storage::NameCurrentAddressFilter {
                address: account,
                relation: relation.unwrap_or(bigname_storage::NameCurrentAddressRelationFilter::Any),
            },
        )),
        (None, None, None, Some(_)) => Err(ApiError {
            status: StatusCode::BAD_REQUEST,
            code: "invalid_input",
            message: "relation requires account, owner, or registrant".to_owned(),
        }),
        (None, None, None, None) => Ok(None),
        _ => Err(ApiError {
            status: StatusCode::BAD_REQUEST,
            code: "invalid_input",
            message: "invalid address relation filter combination".to_owned(),
        }),
    }
}

fn parse_names_sort(sort: Option<&str>) -> ApiResult<bigname_storage::NameCurrentListSort> {
    match sort.unwrap_or("name").trim() {
        "" | "name" => Ok(bigname_storage::NameCurrentListSort::Name),
        "expiry_date" => Ok(bigname_storage::NameCurrentListSort::ExpiryDate),
        "registration_date" => Ok(bigname_storage::NameCurrentListSort::RegistrationDate),
        "created_at" => Ok(bigname_storage::NameCurrentListSort::CreatedAt),
        _ => Err(ApiError {
            status: StatusCode::BAD_REQUEST,
            code: "invalid_input",
            message: "sort must be one of: name, expiry_date, registration_date, created_at"
                .to_owned(),
        }),
    }
}

fn parse_names_order(order: Option<&str>) -> ApiResult<bigname_storage::NameCurrentListOrder> {
    match order.unwrap_or("asc").trim() {
        "" | "asc" => Ok(bigname_storage::NameCurrentListOrder::Asc),
        "desc" => Ok(bigname_storage::NameCurrentListOrder::Desc),
        _ => Err(ApiError {
            status: StatusCode::BAD_REQUEST,
            code: "invalid_input",
            message: "order must be one of: asc, desc".to_owned(),
        }),
    }
}

fn names_sort_label(
    sort: bigname_storage::NameCurrentListSort,
    order: bigname_storage::NameCurrentListOrder,
) -> &'static str {
    match (sort, order) {
        (
            bigname_storage::NameCurrentListSort::Name,
            bigname_storage::NameCurrentListOrder::Asc,
        ) => "name_asc",
        (
            bigname_storage::NameCurrentListSort::Name,
            bigname_storage::NameCurrentListOrder::Desc,
        ) => "name_desc",
        (
            bigname_storage::NameCurrentListSort::ExpiryDate,
            bigname_storage::NameCurrentListOrder::Asc,
        ) => "expiry_date_asc",
        (
            bigname_storage::NameCurrentListSort::ExpiryDate,
            bigname_storage::NameCurrentListOrder::Desc,
        ) => "expiry_date_desc",
        (
            bigname_storage::NameCurrentListSort::RegistrationDate,
            bigname_storage::NameCurrentListOrder::Asc,
        ) => "registration_date_asc",
        (
            bigname_storage::NameCurrentListSort::RegistrationDate,
            bigname_storage::NameCurrentListOrder::Desc,
        ) => "registration_date_desc",
        (
            bigname_storage::NameCurrentListSort::CreatedAt,
            bigname_storage::NameCurrentListOrder::Asc,
        ) => "created_at_asc",
        (
            bigname_storage::NameCurrentListSort::CreatedAt,
            bigname_storage::NameCurrentListOrder::Desc,
        ) => "created_at_desc",
    }
}

fn parse_names_include(include: Option<&str>) -> ApiResult<NamesInclude> {
    let mut parsed = NamesInclude::default();
    for value in include
        .unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        match value {
            "total_count" => parsed.total_count = true,
            "record_summaries" => parsed.record_summaries = true,
            _ => {
                return Err(ApiError {
                    status: StatusCode::BAD_REQUEST,
                    code: "invalid_input",
                    message: "include must contain only record_summaries or total_count".to_owned(),
                });
            }
        }
    }
    Ok(parsed)
}

fn parse_optional_app_relation(
    relation: Option<&str>,
) -> ApiResult<Option<bigname_storage::NameCurrentAddressRelationFilter>> {
    match relation.map(str::trim).filter(|value| !value.is_empty()) {
        None => Ok(None),
        Some("registrant") => Ok(Some(
            bigname_storage::NameCurrentAddressRelationFilter::Relation(
                AddressNameRelation::Registrant,
            ),
        )),
        Some("token_holder") => Ok(Some(
            bigname_storage::NameCurrentAddressRelationFilter::Relation(
                AddressNameRelation::TokenHolder,
            ),
        )),
        Some("effective_controller") => Ok(Some(
            bigname_storage::NameCurrentAddressRelationFilter::Relation(
                AddressNameRelation::EffectiveController,
            ),
        )),
        Some("any") => Ok(Some(bigname_storage::NameCurrentAddressRelationFilter::Any)),
        Some(_) => Err(ApiError {
            status: StatusCode::BAD_REQUEST,
            code: "invalid_input",
            message: "relation must be one of: token_holder, registrant, effective_controller, any"
                .to_owned(),
        }),
    }
}

fn parse_optional_nonempty_query_value(
    value: Option<String>,
    field: &'static str,
) -> ApiResult<Option<String>> {
    let Some(value) = value.map(|value| value.trim().to_owned()) else {
        return Ok(None);
    };
    if value.is_empty() {
        return Ok(None);
    }
    if value.contains(',') {
        return Err(ApiError {
            status: StatusCode::BAD_REQUEST,
            code: "invalid_input",
            message: format!("{field} must not contain comma-separated values"),
        });
    }
    Ok(Some(value))
}

fn parse_optional_exact_name_query_value(
    value: Option<String>,
    field: &'static str,
) -> ApiResult<Option<String>> {
    let Some(value) = value else {
        return Ok(None);
    };
    if value.is_empty() {
        return Ok(None);
    }
    if value.contains(',') {
        return Err(ApiError {
            status: StatusCode::BAD_REQUEST,
            code: "invalid_input",
            message: format!("{field} must not contain comma-separated values"),
        });
    }
    Ok(Some(value))
}

fn parse_optional_address_filter(
    field: &'static str,
    value: Option<&str>,
) -> ApiResult<Option<String>> {
    let Some(value) = value.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(None);
    };
    parse_address_filter_value(field, value).map(Some)
}

fn parse_address_filter_value(field: &'static str, value: &str) -> ApiResult<String> {
    parse_evm_address(value, field)
}

fn unsupported_filter_error(filter: &'static str, reason: &'static str) -> ApiError {
    ApiError {
        status: StatusCode::BAD_REQUEST,
        code: "unsupported",
        message: format!("{filter} is unsupported: {reason}"),
    }
}

fn names_storage_cursor(
    request: &PaginationRequest,
    spec: &CursorSpec,
    sort: bigname_storage::NameCurrentListSort,
    _order: bigname_storage::NameCurrentListOrder,
) -> ApiResult<Option<bigname_storage::NameCurrentListCursor>> {
    let Some(item) = decoded_cursor_item(request, spec)? else {
        return Ok(None);
    };
    require_cursor_item_fields(
        &item,
        &[
            "sort_value",
            "sort_is_null",
            "namespace",
            "normalized_name",
            "namehash",
        ],
    )?;
    let sort_is_null = match item.get("sort_is_null").map(String::as_str) {
        Some("true") => true,
        Some("false") => false,
        _ => return Err(invalid_cursor_error()),
    };
    let sort_value = item.get("sort_value").cloned().unwrap_or_default();
    let sort_value = match sort {
        bigname_storage::NameCurrentListSort::Name => {
            if sort_is_null || sort_value.is_empty() {
                return Err(invalid_cursor_error());
            }
            bigname_storage::NameCurrentListCursorValue::Name(sort_value)
        }
        bigname_storage::NameCurrentListSort::ExpiryDate
        | bigname_storage::NameCurrentListSort::RegistrationDate
        | bigname_storage::NameCurrentListSort::CreatedAt => {
            if sort_is_null {
                bigname_storage::NameCurrentListCursorValue::Timestamp(None)
            } else {
                let parsed =
                    parse_rfc3339_utc_timestamp(&sort_value).map_err(|_| invalid_cursor_error())?;
                bigname_storage::NameCurrentListCursorValue::Timestamp(Some(parsed))
            }
        }
    };

    Ok(Some(bigname_storage::NameCurrentListCursor {
        sort_value,
        namespace: required_cursor_item_field(&item, "namespace")?.to_owned(),
        normalized_name: required_cursor_item_field(&item, "normalized_name")?.to_owned(),
        namehash: required_cursor_item_field(&item, "namehash")?.to_owned(),
    }))
}

fn names_cursor_item(
    cursor: &bigname_storage::NameCurrentListCursor,
    sort: bigname_storage::NameCurrentListSort,
) -> BTreeMap<String, String> {
    let mut item = BTreeMap::new();
    match (&cursor.sort_value, sort) {
        (
            bigname_storage::NameCurrentListCursorValue::Name(value),
            bigname_storage::NameCurrentListSort::Name,
        ) => {
            item.insert("sort_value".to_owned(), value.clone());
            item.insert("sort_is_null".to_owned(), "false".to_owned());
        }
        (
            bigname_storage::NameCurrentListCursorValue::Timestamp(value),
            bigname_storage::NameCurrentListSort::ExpiryDate
            | bigname_storage::NameCurrentListSort::RegistrationDate
            | bigname_storage::NameCurrentListSort::CreatedAt,
        ) => {
            item.insert(
                "sort_value".to_owned(),
                value.map(format_timestamp).unwrap_or_default(),
            );
            item.insert("sort_is_null".to_owned(), value.is_none().to_string());
        }
        _ => {
            item.insert("sort_value".to_owned(), String::new());
            item.insert("sort_is_null".to_owned(), "true".to_owned());
        }
    }
    item.insert("namespace".to_owned(), cursor.namespace.clone());
    item.insert("normalized_name".to_owned(), cursor.normalized_name.clone());
    item.insert("namehash".to_owned(), cursor.namehash.clone());
    item
}
