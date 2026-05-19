use super::*;

pub(super) fn parse_pagination(
    cursor: Option<&str>,
    page_size: Option<u64>,
) -> ApiResult<PaginationRequest> {
    let cursor = cursor
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned);
    let active = cursor.is_some() || page_size.is_some();

    let page_size = match page_size {
        None if !active => DEFAULT_PAGE_SIZE,
        None => DEFAULT_PAGE_SIZE,
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

pub(super) fn paginate_window<T>(
    items: &[T],
    request: &PaginationRequest,
    spec: &CursorSpec,
    item_cursor_fields: impl Fn(&T) -> BTreeMap<String, String>,
) -> ApiResult<PaginationWindow> {
    let start = match request.cursor.as_deref() {
        None => 0,
        Some(cursor) => {
            let decoded = decode_cursor(cursor)?;
            validate_cursor(spec, &decoded)?;
            items
                .iter()
                .position(|item| item_cursor_fields(item) == decoded.item)
                .map(|index| index + 1)
                .ok_or_else(invalid_cursor_error)?
        }
    };
    let end = (start + request.page_size as usize).min(items.len());
    let next_cursor = if end < items.len() {
        Some(encode_cursor(
            &spec.envelope(item_cursor_fields(&items[end - 1])),
        ))
    } else {
        None
    };

    Ok(PaginationWindow {
        start,
        end,
        page: HistoryPageResponse {
            cursor: request.cursor.clone(),
            next_cursor,
            page_size: request.page_size,
            sort: spec.sort.to_owned(),
        },
    })
}

pub(super) fn storage_page_size(request: &PaginationRequest) -> u64 {
    let _ = request.active;
    request.page_size
}

pub(super) fn page_response_from_storage_cursor(
    request: &PaginationRequest,
    spec: &CursorSpec,
    next_cursor_item: Option<BTreeMap<String, String>>,
) -> HistoryPageResponse {
    HistoryPageResponse {
        cursor: request.cursor.clone(),
        next_cursor: next_cursor_item.map(|item| encode_cursor(&spec.envelope(item))),
        page_size: request.page_size,
        sort: spec.sort.to_owned(),
    }
}

pub(super) fn address_names_storage_cursor(
    request: &PaginationRequest,
    spec: &CursorSpec,
) -> ApiResult<Option<bigname_storage::AddressNamesCurrentCursor>> {
    let Some(item) = decoded_cursor_item(request, spec)? else {
        return Ok(None);
    };

    require_cursor_item_fields(
        &item,
        &["canonical_display_name", "logical_name_id", "resource_id"],
    )?;
    let resource_id = Uuid::parse_str(required_cursor_item_field(&item, "resource_id")?)
        .map_err(|_| invalid_cursor_error())?;

    Ok(Some(bigname_storage::AddressNamesCurrentCursor {
        canonical_display_name: required_cursor_item_field(&item, "canonical_display_name")?
            .to_owned(),
        logical_name_id: required_cursor_item_field(&item, "logical_name_id")?.to_owned(),
        resource_id,
    }))
}

pub(super) fn children_storage_cursor(
    request: &PaginationRequest,
    spec: &CursorSpec,
) -> ApiResult<Option<bigname_storage::ChildrenCurrentKeysetCursor>> {
    let Some(item) = decoded_cursor_item(request, spec)? else {
        return Ok(None);
    };

    require_cursor_item_fields(&item, &["canonical_display_name", "child_logical_name_id"])?;
    Ok(Some(bigname_storage::ChildrenCurrentKeysetCursor {
        canonical_display_name: required_cursor_item_field(&item, "canonical_display_name")?
            .to_owned(),
        child_logical_name_id: required_cursor_item_field(&item, "child_logical_name_id")?
            .to_owned(),
    }))
}

pub(super) fn permissions_storage_cursor(
    request: &PaginationRequest,
    spec: &CursorSpec,
) -> ApiResult<Option<bigname_storage::PermissionsCurrentKeysetCursor>> {
    let Some(item) = decoded_cursor_item(request, spec)? else {
        return Ok(None);
    };

    require_cursor_item_fields(&item, &["subject", "scope"])?;
    Ok(Some(bigname_storage::PermissionsCurrentKeysetCursor {
        subject: required_cursor_item_field(&item, "subject")?.to_owned(),
        scope: required_cursor_item_field(&item, "scope")?.to_owned(),
    }))
}

pub(super) fn address_names_cursor_item(
    cursor: &bigname_storage::AddressNamesCurrentCursor,
) -> BTreeMap<String, String> {
    let mut item = BTreeMap::new();
    item.insert(
        "canonical_display_name".to_owned(),
        cursor.canonical_display_name.clone(),
    );
    item.insert("logical_name_id".to_owned(), cursor.logical_name_id.clone());
    item.insert("resource_id".to_owned(), cursor.resource_id.to_string());
    item
}

pub(super) fn children_cursor_item(
    cursor: &bigname_storage::ChildrenCurrentKeysetCursor,
) -> BTreeMap<String, String> {
    let mut item = BTreeMap::new();
    item.insert(
        "canonical_display_name".to_owned(),
        cursor.canonical_display_name.clone(),
    );
    item.insert(
        "child_logical_name_id".to_owned(),
        cursor.child_logical_name_id.clone(),
    );
    item
}

pub(super) fn permissions_cursor_item(
    cursor: &bigname_storage::PermissionsCurrentKeysetCursor,
) -> BTreeMap<String, String> {
    let mut item = BTreeMap::new();
    item.insert("subject".to_owned(), cursor.subject.clone());
    item.insert("scope".to_owned(), cursor.scope.clone());
    item
}

pub(super) async fn ensure_children_cursor_exists(
    pool: &PgPool,
    parent_logical_name_id: &str,
    cursor: &bigname_storage::ChildrenCurrentKeysetCursor,
) -> ApiResult<()> {
    let exists = sqlx::query_scalar::<_, bool>(
        r#"
        SELECT EXISTS (
            SELECT 1
            FROM children_current cc
            JOIN name_surfaces parent
              ON parent.logical_name_id = cc.parent_logical_name_id
            JOIN name_surfaces child
              ON child.logical_name_id = cc.child_logical_name_id
            WHERE cc.parent_logical_name_id = $1
              AND cc.surface_class = 'declared'
              AND cc.canonical_display_name = $2
              AND cc.child_logical_name_id = $3
              AND parent.canonicality_state IN (
                    'canonical'::canonicality_state,
                    'safe'::canonicality_state,
                    'finalized'::canonicality_state
              )
              AND child.canonicality_state IN (
                    'canonical'::canonicality_state,
                    'safe'::canonicality_state,
                    'finalized'::canonicality_state
              )
        )
        "#,
    )
    .bind(parent_logical_name_id)
    .bind(&cursor.canonical_display_name)
    .bind(&cursor.child_logical_name_id)
    .fetch_one(pool)
    .await
    .map_err(|load_error| {
        error!(
            service = "api",
            parent_logical_name_id = %parent_logical_name_id,
            cursor = ?cursor,
            error = ?load_error,
            "failed to validate children_current pagination cursor"
        );
        ApiError::internal_error(format!(
            "failed to load child collection for logical name {parent_logical_name_id}"
        ))
    })?;

    if exists {
        Ok(())
    } else {
        Err(invalid_cursor_error())
    }
}

pub(super) async fn ensure_permissions_cursor_exists(
    pool: &PgPool,
    resource_id: Uuid,
    subject: Option<&str>,
    scope: Option<&PermissionScope>,
    cursor: &bigname_storage::PermissionsCurrentKeysetCursor,
) -> ApiResult<()> {
    let scope_storage_key = scope.map(PermissionScope::storage_key);
    let exists = sqlx::query_scalar::<_, bool>(
        r#"
        SELECT EXISTS (
            SELECT 1
            FROM permissions_current
            WHERE resource_id = $1
              AND ($2::TEXT IS NULL OR subject = $2)
              AND ($3::TEXT IS NULL OR scope = $3)
              AND subject = $4
              AND scope = $5
        )
        "#,
    )
    .bind(resource_id)
    .bind(subject)
    .bind(scope_storage_key.as_deref())
    .bind(&cursor.subject)
    .bind(&cursor.scope)
    .fetch_one(pool)
    .await
    .map_err(|load_error| {
        error!(
            service = "api",
            resource_id = %resource_id,
            subject = ?subject,
            scope = ?scope_storage_key,
            cursor = ?cursor,
            error = ?load_error,
            "failed to validate permissions_current pagination cursor"
        );
        ApiError::internal_error(format!(
            "failed to load permissions for resource {resource_id}"
        ))
    })?;

    if exists {
        Ok(())
    } else {
        Err(invalid_cursor_error())
    }
}

pub(super) fn decoded_cursor_item(
    request: &PaginationRequest,
    spec: &CursorSpec,
) -> ApiResult<Option<BTreeMap<String, String>>> {
    let Some(cursor) = request.cursor.as_deref() else {
        return Ok(None);
    };

    let decoded = decode_cursor(cursor)?;
    validate_cursor(spec, &decoded)?;
    Ok(Some(decoded.item))
}

pub(super) fn required_cursor_item_field<'a>(
    item: &'a BTreeMap<String, String>,
    field: &str,
) -> ApiResult<&'a str> {
    item.get(field)
        .map(String::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(invalid_cursor_error)
}

pub(super) fn require_cursor_item_fields(
    item: &BTreeMap<String, String>,
    expected_fields: &[&str],
) -> ApiResult<()> {
    if item.len() != expected_fields.len()
        || expected_fields
            .iter()
            .any(|field| !item.contains_key(*field))
    {
        return Err(invalid_cursor_error());
    }

    Ok(())
}

pub(super) fn invalid_cursor_error() -> ApiError {
    ApiError {
        status: StatusCode::BAD_REQUEST,
        code: "invalid_input",
        message: "cursor must be a valid pagination cursor".to_owned(),
    }
}

pub(super) fn validate_cursor(spec: &CursorSpec, cursor: &CursorEnvelope) -> ApiResult<()> {
    if cursor.version != CURSOR_VERSION
        || cursor.route != spec.route
        || cursor.anchor != spec.anchor
        || cursor.sort != spec.sort
        || cursor.filters != spec.filters
    {
        return Err(invalid_cursor_error());
    }

    Ok(())
}

pub(super) fn decode_cursor(cursor: &str) -> ApiResult<CursorEnvelope> {
    let decoded = decode_hex(cursor).ok_or_else(invalid_cursor_error)?;
    serde_json::from_slice(&decoded).map_err(|_| invalid_cursor_error())
}

pub(super) fn encode_cursor(cursor: &CursorEnvelope) -> String {
    encode_hex(&serde_json::to_vec(cursor).expect("cursor envelope must serialize for pagination"))
}

pub(super) fn encode_hex(bytes: &[u8]) -> String {
    hex::encode(bytes)
}

pub(super) fn decode_hex(value: &str) -> Option<Vec<u8>> {
    hex::decode(value).ok()
}

pub(super) fn history_cursor_fields(row: &HistoryEvent) -> BTreeMap<String, String> {
    let mut item = BTreeMap::new();
    item.insert(
        "normalized_event_id".to_owned(),
        row.normalized_event_id.to_string(),
    );
    item.insert("event_identity".to_owned(), row.event_identity.clone());
    item
}
