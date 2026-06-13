use super::*;
use alloy_primitives::{Address, hex};

pub(super) fn parse_history_scope(scope: Option<&str>) -> ApiResult<HistoryScope> {
    let scope = scope
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("both");
    match scope {
        "surface" => Ok(HistoryScope::Surface),
        "resource" => Ok(HistoryScope::Resource),
        "both" => Ok(HistoryScope::Both),
        _ => Err(ApiError {
            status: StatusCode::BAD_REQUEST,
            code: "invalid_input",
            message: "scope must be one of: surface, resource, both".to_owned(),
        }),
    }
}

pub(super) fn parse_resolution_mode(mode: Option<&str>) -> ApiResult<ResolutionMode> {
    let mode = mode
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("declared");
    match mode {
        "declared" => Ok(ResolutionMode::Declared),
        "verified" => Ok(ResolutionMode::Verified),
        "both" => Ok(ResolutionMode::Both),
        _ => Err(ApiError {
            status: StatusCode::BAD_REQUEST,
            code: "invalid_input",
            message: "mode must be one of: declared, verified, both".to_owned(),
        }),
    }
}

pub(super) fn parse_response_view(
    view: Option<&str>,
    default: ResponseView,
) -> ApiResult<ResponseView> {
    let Some(view) = view.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(default);
    };

    match view {
        "compact" => Ok(ResponseView::Compact),
        "full" => Ok(ResponseView::Full),
        _ => Err(ApiError {
            status: StatusCode::BAD_REQUEST,
            code: "invalid_input",
            message: "view must be one of: compact, full".to_owned(),
        }),
    }
}

pub(super) fn parse_compact_only_response_view(
    view: Option<&str>,
    full_view_message: &str,
) -> ApiResult<()> {
    if parse_response_view(view, ResponseView::Compact)? == ResponseView::Full {
        return Err(ApiError {
            status: StatusCode::BAD_REQUEST,
            code: "invalid_input",
            message: full_view_message.to_owned(),
        });
    }

    Ok(())
}

pub(super) fn parse_meta_mode(meta: Option<&str>, default: MetaMode) -> ApiResult<MetaMode> {
    let Some(meta) = meta.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(default);
    };

    match meta {
        "none" => Ok(MetaMode::None),
        "summary" => Ok(MetaMode::Summary),
        "full" => Ok(MetaMode::Full),
        _ => Err(ApiError {
            status: StatusCode::BAD_REQUEST,
            code: "invalid_input",
            message: "meta must be one of: none, summary, full".to_owned(),
        }),
    }
}

pub(super) fn parse_primary_name_address(address: &str) -> ApiResult<String> {
    parse_evm_address(address, "address")
}

pub(super) fn parse_evm_address(address: &str, field: &'static str) -> ApiResult<String> {
    if let Some(normalized) = normalize_standard_evm_address(address.trim()) {
        Ok(normalized)
    } else {
        Err(ApiError {
            status: StatusCode::BAD_REQUEST,
            code: "invalid_input",
            message: format!("{field} must be a 0x-prefixed 20-byte hex string"),
        })
    }
}

pub(super) fn parse_exact_name_path_name(namespace: &str, name: &str) -> ApiResult<String> {
    ensure_public_namespace(namespace)?;
    let normalized = bigname_domain::normalization::normalize_name(name).map_err(|error| ApiError {
        status: StatusCode::BAD_REQUEST,
        code: "invalid_input",
        message: error.message().to_owned(),
    })?;
    if normalized.normalized_name != name {
        return Err(ApiError {
            status: StatusCode::BAD_REQUEST,
            code: "invalid_input",
            message: "name path segment must be ENSIP-15 normalized".to_owned(),
        });
    }

    Ok(normalized.normalized_name)
}

pub(super) fn parse_primary_name_namespace(namespace: Option<&str>) -> ApiResult<String> {
    let Some(namespace) = namespace.map(str::trim).filter(|value| !value.is_empty()) else {
        return Err(ApiError {
            status: StatusCode::BAD_REQUEST,
            code: "invalid_input",
            message: "namespace is required".to_owned(),
        });
    };

    ensure_public_namespace(namespace)?;
    Ok(namespace.to_owned())
}

pub(super) fn parse_primary_name_coin_type(coin_type: Option<&str>) -> ApiResult<String> {
    let Some(coin_type) = coin_type.map(str::trim).filter(|value| !value.is_empty()) else {
        return Err(ApiError {
            status: StatusCode::BAD_REQUEST,
            code: "invalid_input",
            message: "coin_type is required".to_owned(),
        });
    };

    if !coin_type.as_bytes().iter().all(u8::is_ascii_digit) {
        return Err(ApiError {
            status: StatusCode::BAD_REQUEST,
            code: "invalid_input",
            message: "coin_type must contain only decimal digits".to_owned(),
        });
    }

    bigname_storage::canonical_addr_coin_type(coin_type).ok_or_else(|| ApiError {
        status: StatusCode::BAD_REQUEST,
        code: "invalid_input",
        message: "coin_type must fit in an unsigned 64-bit integer".to_owned(),
    })
}

pub(super) fn parse_resolution_record_keys(
    records: Option<&str>,
    mode: ResolutionMode,
) -> ApiResult<Vec<ResolutionRecordKey>> {
    let Some(records) = records.map(str::trim).filter(|value| !value.is_empty()) else {
        return if mode.includes_verified() {
            Err(ApiError {
                status: StatusCode::BAD_REQUEST,
                code: "invalid_input",
                message: "records is required when mode is verified or both".to_owned(),
            })
        } else {
            Ok(Vec::new())
        };
    };

    let mut parsed = Vec::new();
    let mut deduped = BTreeSet::new();

    for record_key in records.split(',').map(str::trim) {
        let Some(record) = parse_resolution_record_key(record_key) else {
            return Err(ApiError {
                status: StatusCode::BAD_REQUEST,
                code: "invalid_input",
                message: "records must contain only valid record selectors".to_owned(),
            });
        };

        if mode.includes_verified() && !deduped.insert(record.record_key.clone()) {
            return Err(ApiError {
                status: StatusCode::BAD_REQUEST,
                code: "invalid_input",
                message: "records must not contain duplicate selectors".to_owned(),
            });
        }

        parsed.push(record);
    }

    Ok(parsed)
}

pub(super) fn parse_resolution_record_key(record_key: &str) -> Option<ResolutionRecordKey> {
    if record_key.is_empty()
        || record_key
            .chars()
            .any(|character| character.is_ascii_whitespace() || character == ',')
    {
        return None;
    }

    let is_valid_family = |family: &str| {
        !family.is_empty()
            && family.chars().all(|character| {
                character.is_ascii_lowercase() || character.is_ascii_digit() || character == '_'
            })
    };

    match record_key.split_once(':') {
        None if is_valid_family(record_key) => Some(ResolutionRecordKey {
            record_key: record_key.to_owned(),
            record_family: record_key.to_owned(),
            selector_key: None,
        }),
        Some(("addr", selector)) if !selector.is_empty() => {
            let selector_key = bigname_storage::canonical_addr_coin_type(selector)?;
            Some(ResolutionRecordKey {
                record_key: format!("addr:{selector_key}"),
                record_family: "addr".to_owned(),
                selector_key: Some(selector_key),
            })
        }
        Some((family, selector)) if is_valid_family(family) && !selector.is_empty() => {
            Some(ResolutionRecordKey {
                record_key: record_key.to_owned(),
                record_family: family.to_owned(),
                selector_key: Some(selector.to_owned()),
            })
        }
        _ => None,
    }
}

pub(super) fn parse_permissions_subject(subject: Option<&str>) -> Option<String> {
    subject
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
}

pub(super) fn parse_permission_scope_filter(
    scope: Option<&str>,
) -> ApiResult<Option<PermissionScope>> {
    let Some(scope) = scope.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(None);
    };

    if scope == "root" {
        return Ok(Some(PermissionScope::Root));
    }
    if scope == "registry" {
        return Ok(Some(PermissionScope::Registry));
    }
    if scope == "resource" {
        return Ok(Some(PermissionScope::Resource));
    }

    let mut parts = scope.split(':');
    let kind = parts.next().unwrap_or_default();
    let first = parts.next();
    let second = parts.next();
    let extra = parts.next();

    let parsed = match (kind, first, second, extra) {
        ("resolver", Some(chain_id), Some(resolver_address), None) => {
            Some(PermissionScope::Resolver {
                chain_id: chain_id.to_owned(),
                resolver_address: normalize_address(resolver_address),
            })
        }
        ("record_manager", Some(chain_id), Some(manager_address), None) => {
            Some(PermissionScope::RecordManager {
                chain_id: chain_id.to_owned(),
                manager_address: normalize_address(manager_address),
            })
        }
        ("migration_derived", Some(predecessor_resource_id), None, None) => {
            Some(PermissionScope::MigrationDerived {
                predecessor_resource_id: Uuid::parse_str(predecessor_resource_id).map_err(
                    |_| ApiError {
                        status: StatusCode::BAD_REQUEST,
                        code: "invalid_input",
                        message: "scope must use a valid permissions scope filter".to_owned(),
                    },
                )?,
            })
        }
        ("transport_derived", Some(transport), None, None) => {
            Some(PermissionScope::TransportDerived {
                transport: transport.to_owned(),
            })
        }
        _ => None,
    };

    parsed
        .ok_or(ApiError {
            status: StatusCode::BAD_REQUEST,
            code: "invalid_input",
            message: "scope must use a valid permissions scope filter".to_owned(),
        })
        .map(Some)
}

pub(super) fn parse_children_query(query: &ChildrenQuery) -> ApiResult<bool> {
    parse_children_surface_classes(query.surface_classes.as_deref())?;
    parse_children_include_counts(query.include.as_deref())
}

pub(super) fn parse_address_names_namespace(namespace: Option<&str>) -> ApiResult<Option<String>> {
    let Some(namespace) = namespace.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(None);
    };

    if PUBLIC_NAMESPACES.contains(&namespace) {
        Ok(Some(namespace.to_owned()))
    } else {
        Err(ApiError {
            status: StatusCode::BAD_REQUEST,
            code: "invalid_input",
            message: "namespace must be one of: ens, basenames".to_owned(),
        })
    }
}

pub(super) fn parse_address_name_relation(
    relation: Option<&str>,
) -> ApiResult<Option<AddressNameRelation>> {
    match relation.map(str::trim).filter(|value| !value.is_empty()) {
        None => Ok(None),
        Some("registrant") => Ok(Some(AddressNameRelation::Registrant)),
        Some("token_holder") => Ok(Some(AddressNameRelation::TokenHolder)),
        Some("effective_controller") => Ok(Some(AddressNameRelation::EffectiveController)),
        Some(_) => Err(ApiError {
            status: StatusCode::BAD_REQUEST,
            code: "invalid_input",
            message: "relation must be one of: registrant, token_holder, effective_controller"
                .to_owned(),
        }),
    }
}

pub(super) fn parse_address_names_dedupe_by(
    dedupe_by: Option<&str>,
) -> ApiResult<AddressNamesCurrentDedupe> {
    let dedupe_by = dedupe_by
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("surface");
    match dedupe_by {
        "surface" => Ok(AddressNamesCurrentDedupe::Surface),
        "resource" => Ok(AddressNamesCurrentDedupe::Resource),
        _ => Err(ApiError {
            status: StatusCode::BAD_REQUEST,
            code: "invalid_input",
            message: "dedupe_by must be one of: surface, resource".to_owned(),
        }),
    }
}

pub(super) fn parse_address_names_include(
    include: Option<&str>,
) -> ApiResult<AddressNamesIncludeOptions> {
    let mut options = AddressNamesIncludeOptions::default();

    for value in include
        .unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        match value {
            "role_summary" => options.role_summary = true,
            _ => {
                return Err(ApiError {
                    status: StatusCode::BAD_REQUEST,
                    code: "invalid_input",
                    message: "include must contain only role_summary".to_owned(),
                });
            }
        }
    }

    Ok(options)
}

pub(super) fn parse_children_surface_classes(surface_classes: Option<&str>) -> ApiResult<()> {
    let mut requested_non_declared = false;

    for value in surface_classes
        .unwrap_or("declared")
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        match value {
            "declared" => {}
            "linked" | "alias" | "wildcard" => requested_non_declared = true,
            _ => {
                return Err(ApiError {
                    status: StatusCode::BAD_REQUEST,
                    code: "invalid_input",
                    message:
                        "surface_classes must contain only declared, linked, alias, or wildcard"
                            .to_owned(),
                });
            }
        }
    }

    if requested_non_declared {
        return Err(ApiError {
            status: StatusCode::BAD_REQUEST,
            code: "unsupported",
            message: "surface_classes other than declared are not yet supported".to_owned(),
        });
    }

    Ok(())
}

pub(super) fn parse_children_include_counts(include: Option<&str>) -> ApiResult<bool> {
    let mut include_counts = false;

    for value in include
        .unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        match value {
            "counts" => include_counts = true,
            _ => {
                return Err(ApiError {
                    status: StatusCode::BAD_REQUEST,
                    code: "invalid_input",
                    message: "include must contain only counts".to_owned(),
                });
            }
        }
    }

    Ok(include_counts)
}

pub(super) fn normalize_address(address: &str) -> String {
    normalize_standard_evm_address(address).unwrap_or_else(|| address.to_ascii_lowercase())
}

fn normalize_standard_evm_address(value: &str) -> Option<String> {
    if value.len() != 42 || (!value.starts_with("0x") && !value.starts_with("0X")) {
        return None;
    }

    let address = format!("0x{}", &value[2..]).parse::<Address>().ok()?;
    Some(format_prefixed_hex(address.as_slice()))
}

fn format_prefixed_hex(bytes: impl AsRef<[u8]>) -> String {
    format!("0x{}", hex::encode(bytes))
}

pub(super) fn address_names_dedupe_label(dedupe_by: AddressNamesCurrentDedupe) -> &'static str {
    match dedupe_by {
        AddressNamesCurrentDedupe::Surface => "surface",
        AddressNamesCurrentDedupe::Resource => "resource",
    }
}

pub(super) fn ensure_public_namespace(namespace: &str) -> ApiResult<()> {
    if PUBLIC_NAMESPACES.contains(&namespace) {
        Ok(())
    } else {
        Err(ApiError {
            status: StatusCode::NOT_FOUND,
            code: "not_found",
            message: format!("namespace {namespace} is not supported"),
        })
    }
}

pub(super) fn collect_unique(values: impl Iterator<Item = String>) -> Vec<String> {
    values.collect::<BTreeSet<_>>().into_iter().collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_address_uses_alloy_for_standard_hex_without_tightening_fallbacks() {
        assert_eq!(
            normalize_address("0X00000000000C2E074eC69A0dFb2997BA6C7d2E1E"),
            "0x00000000000c2e074ec69a0dfb2997ba6c7d2e1e"
        );
        assert_eq!(normalize_address("NOT-A-HEX-ADDRESS"), "not-a-hex-address");
        assert_eq!(normalize_address("0xABC"), "0xabc");
        assert_eq!(
            normalize_address("00000000000000000000000000000000000000AA"),
            "00000000000000000000000000000000000000aa"
        );
    }

    #[test]
    fn parse_primary_name_address_keeps_existing_validation_boundary() {
        let parsed = match parse_primary_name_address(
            " 0X00000000000C2E074eC69A0dFb2997BA6C7d2E1E ",
        ) {
            Ok(parsed) => parsed,
            Err(_) => panic!("standard address should parse"),
        };
        assert_eq!(
            parsed,
            "0x00000000000c2e074ec69a0dfb2997ba6c7d2e1e"
        );
        assert!(parse_primary_name_address("0xABC").is_err());
        assert!(parse_primary_name_address("00000000000000000000000000000000000000AA").is_err());
    }

    #[test]
    fn primary_name_coin_type_is_canonicalized_at_parse_boundary() {
        assert_eq!(
            must_parse(parse_primary_name_coin_type(Some("060"))),
            "60"
        );
    }

    #[test]
    fn empty_optional_enum_values_use_route_defaults() {
        assert_eq!(must_parse(parse_history_scope(Some(""))), HistoryScope::Both);
        assert_eq!(
            must_parse(parse_resolution_mode(Some(""))),
            ResolutionMode::Declared
        );
        assert_eq!(
            must_parse(parse_response_view(Some(""), ResponseView::Full)),
            ResponseView::Full
        );
        assert_eq!(
            must_parse(parse_meta_mode(Some(""), MetaMode::Full)),
            MetaMode::Full
        );
        assert_eq!(
            must_parse(parse_address_names_dedupe_by(Some(""))),
            AddressNamesCurrentDedupe::Surface
        );
    }

    #[test]
    fn exact_name_path_names_reject_non_normalized_or_unnormalizable_input() {
        assert!(parse_exact_name_path_name("ens", "Alice.eth").is_err());
        assert_eq!(
            must_parse(parse_exact_name_path_name("ens", "alice.eth")),
            "alice.eth"
        );
        assert!(parse_exact_name_path_name("ens", "bad name.eth").is_err());
    }

    #[test]
    fn resolution_record_keys_reject_overflowing_addr_coin_type() {
        let error = parse_resolution_record_keys(
            Some("addr:18446744073709551616"),
            ResolutionMode::Verified,
        )
        .expect_err("overflowing coin_type selectors must be invalid input");

        assert_eq!(error.status, StatusCode::BAD_REQUEST);
        assert_eq!(error.code, "invalid_input");
    }

    #[test]
    fn resolution_record_keys_canonicalize_addr_coin_type_before_dedupe() {
        let error = parse_resolution_record_keys(Some("addr:060,addr:60"), ResolutionMode::Verified)
            .expect_err("canonical duplicate addr selectors must be rejected");

        assert_eq!(error.status, StatusCode::BAD_REQUEST);
        assert_eq!(error.code, "invalid_input");
        assert_eq!(error.message, "records must not contain duplicate selectors");
    }

    fn must_parse<T>(result: ApiResult<T>) -> T {
        match result {
            Ok(value) => value,
            Err(_) => panic!("parser should accept empty optional enum value"),
        }
    }
}
