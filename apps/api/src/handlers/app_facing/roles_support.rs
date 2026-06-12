fn reject_role_bitmap_filter(role_bitmap: Option<&str>) -> ApiResult<()> {
    if role_bitmap
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .is_some()
    {
        return Err(ApiError {
            status: StatusCode::BAD_REQUEST,
            code: "unsupported",
            message: "role_bitmap filters are unsupported because permissions_current does not project raw role bitmaps; use effective_powers from response rows instead".to_owned(),
        });
    }

    Ok(())
}

const ENSV2_ROOT_UPSTREAM_RESOURCE: &str =
    "0x0000000000000000000000000000000000000000000000000000000000000000";
const ENSV2_RESOURCE_UUID_PREFIX: &str = "ens-v2-resource";
const ENSV2_ROOT_SOURCE_FAMILY: &str = "ens_v2_root_l1";
const ENSV2_REGISTRY_SOURCE_FAMILY: &str = "ens_v2_registry_l1";

fn parse_roles_account(account: Option<&str>) -> Option<String> {
    parse_permissions_subject(account).map(|account| account.to_ascii_lowercase())
}

fn parse_optional_roles_resource_id(resource_id: Option<&str>) -> ApiResult<Option<Uuid>> {
    let Some(resource_id) = resource_id
        .map(str::trim)
        .filter(|value| !value.is_empty())
    else {
        return Ok(None);
    };

    Uuid::parse_str(resource_id)
        .map(Some)
        .map_err(|_| ApiError {
            status: StatusCode::BAD_REQUEST,
            code: "invalid_input",
            message: "resource_id must be a UUID".to_owned(),
        })
}

fn parse_roles_name_filter(
    namespace: Option<&str>,
    name: Option<&str>,
) -> ApiResult<Option<(String, String)>> {
    let namespace = namespace.map(str::trim).filter(|value| !value.is_empty());
    let name = parse_optional_exact_name_query_value(name);

    match (namespace, name) {
        (None, None) => Ok(None),
        (Some(namespace), Some(name)) => {
            ensure_public_namespace(namespace)?;
            Ok(Some((namespace.to_owned(), name.to_owned())))
        }
        _ => Err(ApiError {
            status: StatusCode::BAD_REQUEST,
            code: "invalid_input",
            message: "namespace and name must be supplied together".to_owned(),
        }),
    }
}

fn parse_required_resource_lookup_name(
    namespace: Option<&str>,
    name: Option<&str>,
) -> ApiResult<(String, String)> {
    let namespace = namespace.map(str::trim).filter(|value| !value.is_empty());
    let name = parse_optional_exact_name_query_value(name);

    match (namespace, name) {
        (Some(namespace), Some(name)) => {
            ensure_public_namespace(namespace)?;
            Ok((namespace.to_owned(), name.to_owned()))
        }
        (None, _) => Err(ApiError {
            status: StatusCode::BAD_REQUEST,
            code: "invalid_input",
            message: "namespace is required".to_owned(),
        }),
        (_, None) => Err(ApiError {
            status: StatusCode::BAD_REQUEST,
            code: "invalid_input",
            message: "name is required".to_owned(),
        }),
    }
}

fn parse_optional_exact_name_query_value(name: Option<&str>) -> Option<&str> {
    name.filter(|value| !value.is_empty())
}

async fn ensure_permissions_current_projection_available(
    pool: &PgPool,
    route: &'static str,
) -> ApiResult<()> {
    let available = sqlx::query_scalar::<_, bool>(
        r#"
        SELECT
            EXISTS (
                SELECT 1
                FROM current_projection_replay_status
                WHERE projection = 'permissions_current'
            )
            OR EXISTS (
                SELECT 1
                FROM projection_apply_cursors
            )
        "#,
    )
    .fetch_one(pool)
    .await
    .map_err(|load_error| {
        error!(
            service = "api",
            route = route,
            error = ?load_error,
            "failed to check permissions_current projection readiness for roles route"
        );
        ApiError::internal_error("failed to check roles projection readiness")
    })?;

    if available {
        Ok(())
    } else {
        Err(ApiError {
            status: StatusCode::CONFLICT,
            code: "stale",
            message: "permissions_current projection is not yet available for roles".to_owned(),
        })
    }
}

async fn load_resource_lookup_row(
    pool: &PgPool,
    namespace: &str,
    name: &str,
) -> ApiResult<NameCurrentRow> {
    let logical_name_id = format!("{namespace}:{name}");
    let row = load_name_current(pool, &logical_name_id)
        .await
        .map_err(|load_error| {
            error!(
                service = "api",
                namespace = %namespace,
                name = %name,
                logical_name_id = %logical_name_id,
                error = ?load_error,
                "failed to load name_current row for resource lookup"
            );
            ApiError::internal_error(format!(
                "failed to resolve current resource for name {namespace}/{name}"
            ))
        })?;

    row.ok_or_else(|| name_not_found_error(namespace, name))
}

fn resource_id_from_name_current(row: &NameCurrentRow) -> ApiResult<Uuid> {
    row.resource_id.ok_or_else(|| ApiError {
        status: StatusCode::NOT_FOUND,
        code: "not_found",
        message: format!(
            "current resource for name {} was not found in namespace {}",
            row.normalized_name, row.namespace
        ),
    })
}

fn roles_cursor_spec(
    route: &'static str,
    anchor: String,
    sort: &'static str,
    account: Option<&str>,
    resource_id: Option<Uuid>,
    name_filter: Option<&(String, String)>,
) -> CursorSpec {
    let mut filters = BTreeMap::new();
    if let Some(account) = account {
        filters.insert("account".to_owned(), account.to_owned());
    }
    if let Some(resource_id) = resource_id {
        filters.insert("resource_id".to_owned(), resource_id.to_string());
    }
    if let Some((namespace, name)) = name_filter {
        filters.insert("namespace".to_owned(), namespace.clone());
        filters.insert("name".to_owned(), name.clone());
    }

    CursorSpec {
        route,
        anchor,
        sort,
        filters,
    }
}

fn roles_storage_cursor(
    request: &PaginationRequest,
    spec: &CursorSpec,
) -> ApiResult<Option<bigname_storage::PermissionsCurrentAccountResourceCursor>> {
    let Some(item) = decoded_cursor_item(request, spec)? else {
        return Ok(None);
    };

    require_cursor_item_fields(&item, &["account", "resource_id", "scope"])?;
    let resource_id = Uuid::parse_str(required_cursor_item_field(&item, "resource_id")?)
        .map_err(|_| invalid_cursor_error())?;

    Ok(Some(
        bigname_storage::PermissionsCurrentAccountResourceCursor {
            subject: required_cursor_item_field(&item, "account")?.to_owned(),
            resource_id,
            scope: required_cursor_item_field(&item, "scope")?.to_owned(),
        },
    ))
}

fn roles_cursor_item(
    cursor: &bigname_storage::PermissionsCurrentAccountResourceCursor,
) -> BTreeMap<String, String> {
    let mut item = BTreeMap::new();
    item.insert("account".to_owned(), cursor.subject.clone());
    item.insert("resource_id".to_owned(), cursor.resource_id.to_string());
    item.insert("scope".to_owned(), cursor.scope.clone());
    item
}

async fn ensure_roles_cursor_exists(
    pool: &PgPool,
    account: Option<&str>,
    resource_id: Option<Uuid>,
    cursor: &bigname_storage::PermissionsCurrentAccountResourceCursor,
    route: &'static str,
) -> ApiResult<()> {
    let exists = sqlx::query_scalar::<_, bool>(
        r#"
        SELECT EXISTS (
            SELECT 1
            FROM permissions_current pc
            JOIN resources resource
              ON resource.resource_id = pc.resource_id
            WHERE ($1::TEXT IS NULL OR pc.subject = $1)
              AND ($2::UUID IS NULL OR pc.resource_id = $2)
              AND pc.subject = $3
              AND pc.resource_id = $4
              AND pc.scope = $5
              AND resource.canonicality_state IN (
                    'canonical'::canonicality_state,
                    'safe'::canonicality_state,
                    'finalized'::canonicality_state
              )
        )
        "#,
    )
    .bind(account)
    .bind(resource_id)
    .bind(&cursor.subject)
    .bind(cursor.resource_id)
    .bind(&cursor.scope)
    .fetch_one(pool)
    .await
    .map_err(|load_error| {
        error!(
            service = "api",
            route = route,
            account = ?account,
            resource_id = ?resource_id,
            cursor = ?cursor,
            error = ?load_error,
            "failed to validate app-facing roles pagination cursor"
        );
        ApiError::internal_error("failed to load app-facing roles cursor")
    })?;

    if exists {
        Ok(())
    } else {
        Err(invalid_cursor_error())
    }
}

async fn ensure_composed_roles_cursor_exists(
    pool: &PgPool,
    account: Option<&str>,
    resource_id: Option<Uuid>,
    root_resource_id: Option<Uuid>,
    cursor: &bigname_storage::PermissionsCurrentAccountResourceCursor,
    route: &'static str,
) -> ApiResult<()> {
    if let (Some(resource_id), Some(root_resource_id)) = (resource_id, root_resource_id) {
        if cursor.resource_id != resource_id && cursor.resource_id != root_resource_id {
            return Err(invalid_cursor_error());
        }
        return ensure_roles_cursor_exists(pool, account, Some(cursor.resource_id), cursor, route)
            .await;
    }

    ensure_roles_cursor_exists(pool, account, resource_id, cursor, route).await
}

async fn load_roles_page(
    pool: &PgPool,
    account: Option<&str>,
    resource_id: Option<Uuid>,
    cursor: Option<&bigname_storage::PermissionsCurrentAccountResourceCursor>,
    page_size: u64,
    route: &'static str,
) -> ApiResult<bigname_storage::PermissionsCurrentAccountResourcePage> {
    bigname_storage::load_permissions_current_account_resource_page(
        pool,
        account,
        resource_id,
        cursor,
        page_size,
    )
    .await
    .map_err(|load_error| {
        error!(
            service = "api",
            route = route,
            account = ?account,
            resource_id = ?resource_id,
            error = ?load_error,
            "failed to load permissions_current rows for app-facing roles route"
        );
        ApiError::internal_error("failed to load app-facing roles")
    })
}

async fn load_composed_roles_page(
    pool: &PgPool,
    account: Option<&str>,
    resource_id: Option<Uuid>,
    root_resource_id: Option<Uuid>,
    cursor: Option<&bigname_storage::PermissionsCurrentAccountResourceCursor>,
    page_size: u64,
    route: &'static str,
) -> ApiResult<bigname_storage::PermissionsCurrentAccountResourcePage> {
    let Some(resource_id) = resource_id else {
        return load_roles_page(pool, account, None, cursor, page_size, route).await;
    };
    let Some(root_resource_id) = root_resource_id.filter(|root| *root != resource_id) else {
        return load_roles_page(pool, account, Some(resource_id), cursor, page_size, route).await;
    };

    let fetch_size = page_size.saturating_add(1);
    let resource_page =
        load_roles_page(pool, account, Some(resource_id), cursor, fetch_size, route).await?;
    let root_page =
        load_roles_page(pool, account, Some(root_resource_id), cursor, fetch_size, route).await?;

    Ok(merge_roles_pages(resource_page, root_page, page_size))
}

fn merge_roles_pages(
    resource_page: bigname_storage::PermissionsCurrentAccountResourcePage,
    root_page: bigname_storage::PermissionsCurrentAccountResourcePage,
    page_size: u64,
) -> bigname_storage::PermissionsCurrentAccountResourcePage {
    let bigname_storage::PermissionsCurrentAccountResourcePage {
        rows: mut resource_rows,
        next_cursor: _,
        summary: resource_summary,
    } = resource_page;
    let bigname_storage::PermissionsCurrentAccountResourcePage {
        rows: root_rows,
        next_cursor: _,
        summary: root_summary,
    } = root_page;

    resource_rows.extend(root_rows);
    resource_rows.sort_by(|left, right| {
        left.subject
            .cmp(&right.subject)
            .then_with(|| left.resource_id.cmp(&right.resource_id))
            .then_with(|| left.scope.storage_key().cmp(&right.scope.storage_key()))
    });

    let page_size = usize::try_from(page_size).expect("bounded role page_size must fit usize");
    let next_cursor = if resource_rows.len() > page_size {
        resource_rows.truncate(page_size);
        resource_rows
            .last()
            .map(bigname_storage::PermissionsCurrentAccountResourceCursor::from)
    } else {
        None
    };

    bigname_storage::PermissionsCurrentAccountResourcePage {
        rows: resource_rows,
        next_cursor,
        summary: merge_role_summaries(resource_summary, root_summary),
    }
}

fn merge_role_summaries(
    mut resource_summary: bigname_storage::PermissionsCurrentFullFilterSummary,
    root_summary: bigname_storage::PermissionsCurrentFullFilterSummary,
) -> bigname_storage::PermissionsCurrentFullFilterSummary {
    resource_summary.row_count = resource_summary
        .row_count
        .saturating_add(root_summary.row_count);
    if resource_summary.coverage.is_none() {
        resource_summary.coverage = root_summary.coverage;
    }
    resource_summary.provenance.extend(root_summary.provenance);
    resource_summary
        .chain_positions
        .extend(root_summary.chain_positions);
    resource_summary
        .canonicality_summaries
        .extend(root_summary.canonicality_summaries);
    resource_summary.last_recomputed_at =
        match (resource_summary.last_recomputed_at, root_summary.last_recomputed_at) {
            (Some(resource), Some(root)) => Some(resource.max(root)),
            (Some(resource), None) => Some(resource),
            (None, Some(root)) => Some(root),
            (None, None) => None,
        };
    resource_summary
}

async fn load_ensv2_root_resource_id_for_name_resource(
    pool: &PgPool,
    resource_id: Uuid,
    route: &'static str,
) -> ApiResult<Option<Uuid>> {
    let Some(resource) = load_resource(pool, resource_id).await.map_err(|load_error| {
        error!(
            service = "api",
            route = route,
            resource_id = %resource_id,
            error = ?load_error,
            "failed to load resource provenance for ENSv2 root fallback roles"
        );
        ApiError::internal_error("failed to load resource provenance for roles")
    })?
    else {
        return Ok(None);
    };

    Ok(ensv2_root_resource_id_from_resource(&resource).filter(|root| *root != resource_id))
}

fn ensv2_root_resource_id_from_resource(resource: &bigname_storage::Resource) -> Option<Uuid> {
    let provenance = resource.provenance.as_object()?;
    let source_family = provenance.get("source_family")?.as_str()?;
    if !matches!(
        source_family,
        ENSV2_ROOT_SOURCE_FAMILY | ENSV2_REGISTRY_SOURCE_FAMILY
    ) {
        return None;
    }

    let chain_id = provenance.get("chain_id")?.as_str()?;
    let registry_contract_instance_id = provenance
        .get("registry_contract_instance_id")?
        .as_str()
        .and_then(|value| Uuid::parse_str(value).ok())?;
    provenance.get("upstream_resource")?.as_str()?;

    Some(ensv2_registry_permission_resource_id(
        chain_id,
        registry_contract_instance_id,
        ENSV2_ROOT_UPSTREAM_RESOURCE,
    ))
}

fn ensv2_registry_permission_resource_id(
    chain_id: &str,
    registry_contract_instance_id: Uuid,
    upstream_resource: &str,
) -> Uuid {
    let seed =
        format!("{ENSV2_RESOURCE_UUID_PREFIX}:{chain_id}:{registry_contract_instance_id}:{upstream_resource}");
    let digest = alloy_primitives::keccak256(seed.as_bytes());
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    bytes[6] = (bytes[6] & 0x0f) | 0x50;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Uuid::from_bytes(bytes)
}

async fn load_associated_role_names(
    pool: &PgPool,
    rows: &[PermissionsCurrentRow],
    resolved_name_row: Option<&NameCurrentRow>,
) -> ApiResult<BTreeMap<Uuid, String>> {
    let mut associated_names = BTreeMap::new();
    if let Some(row) = resolved_name_row
        && let Some(resource_id) = row.resource_id
    {
        associated_names.insert(resource_id, row.canonical_display_name.clone());
    }

    let missing_resource_ids = rows
        .iter()
        .map(|row| row.resource_id)
        .filter(|resource_id| !associated_names.contains_key(resource_id))
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    if missing_resource_ids.is_empty() {
        return Ok(associated_names);
    }

    let loaded = sqlx::query(
        r#"
        SELECT DISTINCT ON (nc.resource_id)
            nc.resource_id,
            nc.canonical_display_name
        FROM name_current nc
        JOIN name_surfaces surface
          ON surface.logical_name_id = nc.logical_name_id
        LEFT JOIN resources resource
          ON resource.resource_id = nc.resource_id
        LEFT JOIN surface_bindings binding
          ON binding.surface_binding_id = nc.surface_binding_id
        LEFT JOIN token_lineages token_lineage
          ON token_lineage.token_lineage_id = nc.token_lineage_id
        WHERE nc.resource_id = ANY($1::UUID[])
          AND surface.canonicality_state IN (
              'canonical'::canonicality_state,
              'safe'::canonicality_state,
              'finalized'::canonicality_state
          )
          AND (
              nc.surface_binding_id IS NULL
              OR (
                  resource.canonicality_state IN (
                      'canonical'::canonicality_state,
                      'safe'::canonicality_state,
                      'finalized'::canonicality_state
                  )
                  AND binding.canonicality_state IN (
                      'canonical'::canonicality_state,
                      'safe'::canonicality_state,
                      'finalized'::canonicality_state
                  )
                  AND binding.active_to IS NULL
                  AND (
                      nc.token_lineage_id IS NULL
                      OR token_lineage.canonicality_state IN (
                          'canonical'::canonicality_state,
                          'safe'::canonicality_state,
                          'finalized'::canonicality_state
                      )
                  )
              )
          )
        ORDER BY nc.resource_id ASC, nc.namespace ASC, nc.canonical_display_name ASC, nc.logical_name_id ASC
        "#,
    )
    .bind(&missing_resource_ids)
    .fetch_all(pool)
    .await
    .map_err(|load_error| {
        error!(
            service = "api",
            resource_ids = ?missing_resource_ids,
            error = ?load_error,
            "failed to load associated name_current rows for app-facing roles"
        );
        ApiError::internal_error("failed to load associated names for app-facing roles")
    })?;

    for row in loaded {
        let resource_id: Uuid = row.try_get("resource_id").map_err(|load_error| {
            error!(
                service = "api",
                error = ?load_error,
                "failed to decode associated role resource_id"
            );
            ApiError::internal_error("failed to load associated names for app-facing roles")
        })?;
        let canonical_display_name: String =
            row.try_get("canonical_display_name").map_err(|load_error| {
                error!(
                    service = "api",
                    error = ?load_error,
                    "failed to decode associated role name"
                );
                ApiError::internal_error("failed to load associated names for app-facing roles")
            })?;
        associated_names.insert(resource_id, canonical_display_name);
    }

    Ok(associated_names)
}
