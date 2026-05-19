use super::*;

struct ParsedEventsFilter {
    storage_filter: EventHistoryFilter,
    cursor_filters: BTreeMap<String, String>,
}

pub(super) async fn events(
    Query(query): Query<EventsQuery>,
    State(state): State<AppState>,
) -> ApiResult<Json<CompactEventsResponse>> {
    parse_compact_only_response_view(
        query.view.as_deref(),
        "view=full is reserved for /v1/events until the full event shape is documented",
    )?;
    let meta = parse_meta_mode(query.meta.as_deref(), MetaMode::Summary)?;
    let parsed = parse_events_filter(&query)?;
    let pagination = parse_pagination(query.cursor.as_deref(), query.page_size)?;

    let rows = load_event_history(&state.pool, parsed.storage_filter, true)
        .await
        .map_err(|load_error| {
            error!(
                service = "api",
                error = ?load_error,
                "failed to load compact events"
            );
            ApiError::internal_error("failed to load compact events")
        })?;

    let page = paginate_window(
        &rows,
        &pagination,
        &CursorSpec {
            route: "/v1/events",
            anchor: "events".to_owned(),
            sort: "chain_position_desc",
            filters: parsed.cursor_filters,
        },
        history_cursor_fields,
    )?;

    Ok(Json(build_compact_events_response(
        &rows,
        &rows[page.start..page.end],
        page.page,
        meta,
        HistoryScope::Both,
    )))
}

fn parse_events_filter(query: &EventsQuery) -> ApiResult<ParsedEventsFilter> {
    reject_unsupported_event_filters(query)?;

    let namespace = parse_address_names_namespace(query.namespace.as_deref())?;
    let name = trimmed_query_value(query.name.as_deref());
    if name.is_some() && namespace.is_none() {
        return Err(ApiError {
            status: StatusCode::BAD_REQUEST,
            code: "invalid_input",
            message: "name requires namespace for /v1/events".to_owned(),
        });
    }
    let logical_name_id = name
        .as_ref()
        .map(|name| format!("{}:{name}", namespace.as_ref().expect("namespace checked")));

    let resource_id = parse_events_resource_id(query)?;
    let address = trimmed_query_value(query.address.as_deref()).map(|address| normalize_address(&address));
    let relation = parse_events_relation(query.relation.as_deref())?;
    if relation.is_some() && address.is_none() {
        return Err(ApiError {
            status: StatusCode::BAD_REQUEST,
            code: "invalid_input",
            message: "relation requires address for /v1/events".to_owned(),
        });
    }

    let from_block = parse_event_block_bound(query.from_block.as_deref(), "from_block")?;
    let to_block = parse_event_block_bound(query.to_block.as_deref(), "to_block")?;
    if matches!(
        (from_block, to_block),
        (Some(from_block), Some(to_block)) if from_block > to_block
    ) {
        return Err(ApiError {
            status: StatusCode::BAD_REQUEST,
            code: "invalid_input",
            message: "from_block must be less than or equal to to_block".to_owned(),
        });
    }

    let event_type = trimmed_query_value(query.event_type.as_deref());
    let event_kinds = parse_event_type_filter(event_type.as_deref())?;

    let mut cursor_filters = BTreeMap::new();
    if let Some(namespace) = namespace.as_ref() {
        cursor_filters.insert("namespace".to_owned(), namespace.clone());
    }
    if let Some(name) = name.as_ref() {
        cursor_filters.insert("name".to_owned(), name.clone());
    }
    if let Some(address) = address.as_ref() {
        cursor_filters.insert("address".to_owned(), address.clone());
    }
    if let Some(resource_id) = resource_id {
        cursor_filters.insert("resource_id".to_owned(), resource_id.to_string());
    }
    if let Some(event_type) = event_type.as_ref() {
        cursor_filters.insert("type".to_owned(), event_type.clone());
    }
    if let Some(relation) = relation {
        cursor_filters.insert("relation".to_owned(), relation.as_str().to_owned());
    } else if address.is_some() && trimmed_query_value(query.relation.as_deref()).as_deref() == Some("any") {
        cursor_filters.insert("relation".to_owned(), "any".to_owned());
    }
    if let Some(from_block) = from_block {
        cursor_filters.insert("from_block".to_owned(), from_block.to_string());
    }
    if let Some(to_block) = to_block {
        cursor_filters.insert("to_block".to_owned(), to_block.to_string());
    }

    Ok(ParsedEventsFilter {
        storage_filter: EventHistoryFilter {
            namespace,
            logical_name_id,
            resource_id,
            address: address.map(|address| EventHistoryAddressFilter { address, relation }),
            event_kinds,
            from_block,
            to_block,
        },
        cursor_filters,
    })
}

fn reject_unsupported_event_filters(query: &EventsQuery) -> ApiResult<()> {
    if trimmed_query_value(query.resource_hex.as_deref()).is_some() {
        return Err(unsupported_events_filter(
            "resource_hex is not supported by /v1/events; use resource_id",
        ));
    }

    for (field_name, value) in [
        ("selector", query.selector.as_deref()),
        ("selector_key", query.selector_key.as_deref()),
        ("record", query.record.as_deref()),
        ("record_key", query.record_key.as_deref()),
        ("records", query.records.as_deref()),
        ("texts", query.texts.as_deref()),
        ("text_key", query.text_key.as_deref()),
        ("coin_type", query.coin_type.as_deref()),
        ("coin_types", query.coin_types.as_deref()),
        ("avatar", query.avatar.as_deref()),
        ("content_hash", query.content_hash.as_deref()),
    ] {
        if trimmed_query_value(value).is_some() {
            return Err(unsupported_events_filter(&format!(
                "{field_name} is selector-exact record history and is not supported by /v1/events"
            )));
        }
    }

    Ok(())
}

fn parse_events_resource_id(query: &EventsQuery) -> ApiResult<Option<Uuid>> {
    let resource = trimmed_query_value(query.resource.as_deref());
    let resource_id = trimmed_query_value(query.resource_id.as_deref());
    if resource.is_some() && resource_id.is_some() {
        return Err(ApiError {
            status: StatusCode::BAD_REQUEST,
            code: "invalid_input",
            message: "resource and resource_id are mutually exclusive".to_owned(),
        });
    }

    resource
        .or(resource_id)
        .map(|value| {
            Uuid::parse_str(&value).map_err(|_| ApiError {
                status: StatusCode::BAD_REQUEST,
                code: "invalid_input",
                message: "resource_id must be a UUID".to_owned(),
            })
        })
        .transpose()
}

fn parse_events_relation(relation: Option<&str>) -> ApiResult<Option<AddressNameRelation>> {
    match trimmed_query_value(relation).as_deref() {
        None | Some("any") => Ok(None),
        Some("registrant") => Ok(Some(AddressNameRelation::Registrant)),
        Some("token_holder") => Ok(Some(AddressNameRelation::TokenHolder)),
        Some("effective_controller") => Ok(Some(AddressNameRelation::EffectiveController)),
        Some(_) => Err(ApiError {
            status: StatusCode::BAD_REQUEST,
            code: "invalid_input",
            message: "relation must be one of: any, registrant, token_holder, effective_controller"
                .to_owned(),
        }),
    }
}

fn parse_event_block_bound(value: Option<&str>, field_name: &str) -> ApiResult<Option<i64>> {
    let Some(value) = trimmed_query_value(value) else {
        return Ok(None);
    };

    value
        .parse::<i64>()
        .ok()
        .filter(|value| *value >= 0)
        .ok_or_else(|| ApiError {
            status: StatusCode::BAD_REQUEST,
            code: "invalid_input",
            message: format!("{field_name} must be a non-negative integer"),
        })
        .map(Some)
}

fn parse_event_type_filter(value: Option<&str>) -> ApiResult<Vec<String>> {
    let Some(value) = value else {
        return Ok(Vec::new());
    };

    let event_kinds = match value {
        "registration" | "registered" => vec!["RegistrationGranted", "LabelRegistered"],
        "transfer" => vec!["TokenControlTransferred"],
        "authority" => vec!["AuthorityTransferred"],
        "resolver" => vec!["ResolverChanged"],
        "record" => vec!["RecordChanged", "RecordVersionChanged"],
        "primary_name" | "reverse" => vec!["ReverseChanged"],
        "permission" | "role" => vec![
            "PermissionChanged",
            "PermissionScopeChanged",
            "RolesChanged",
            "EACRolesChanged",
        ],
        exact if is_normalized_event_kind(exact) => return Ok(vec![exact.to_owned()]),
        alias => {
            return Err(unsupported_events_filter(&format!(
                "event type alias {alias} is not supported"
            )));
        }
    };

    Ok(event_kinds.into_iter().map(str::to_owned).collect())
}

fn is_normalized_event_kind(value: &str) -> bool {
    value
        .as_bytes()
        .first()
        .is_some_and(u8::is_ascii_uppercase)
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
}

fn unsupported_events_filter(message: &str) -> ApiError {
    ApiError {
        status: StatusCode::BAD_REQUEST,
        code: "unsupported",
        message: message.to_owned(),
    }
}

fn trimmed_query_value(value: Option<&str>) -> Option<String> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
}
