pub(crate) type CompactNameRecordsResponse = JsonValue;

const COMPACT_RECORDS_VERIFIED_UNSUPPORTED_REASON: &str =
    "verified compact record read is not available for this selector";
const COMPACT_RECORDS_DECLARED_INVENTORY_UNSUPPORTED_REASON: &str =
    "declared compact record inventory is not yet projected";
const COMPACT_RECORDS_DECLARED_CACHE_UNSUPPORTED_REASON: &str =
    "declared compact record cache is not yet projected";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CompactNameRecordsMode {
    Auto,
    Declared,
    Verified,
    Both,
}

impl CompactNameRecordsMode {
    pub(crate) fn includes_declared(self) -> bool {
        matches!(self, Self::Auto | Self::Declared | Self::Both)
    }

    pub(crate) fn includes_verified(self) -> bool {
        matches!(self, Self::Auto | Self::Verified | Self::Both)
    }

    fn label(self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::Declared => "declared",
            Self::Verified => "verified",
            Self::Both => "both",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CompactNameRecordsDefaultMode {
    Auto,
    Declared,
}

impl CompactNameRecordsDefaultMode {
    fn mode(self) -> CompactNameRecordsMode {
        match self {
            Self::Auto => CompactNameRecordsMode::Auto,
            Self::Declared => CompactNameRecordsMode::Declared,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CompactNameRecordsValueSource {
    Declared,
    Verified,
}

#[derive(Clone, Debug)]
pub(crate) struct CompactNameRecordsRequest {
    pub(crate) mode: CompactNameRecordsMode,
    meta: MetaMode,
    texts: Vec<String>,
    known_text_keys: bool,
    avatar: bool,
    content_hash: bool,
    coin_types: Vec<String>,
    include: CompactNameRecordsInclude,
}

#[derive(Clone, Copy, Debug, Default)]
struct CompactNameRecordsInclude {
    resolver_address: bool,
    known_text_keys: bool,
    avatar: bool,
    content_hash: bool,
    coins: bool,
}

#[derive(Clone, Debug, Default)]
struct CompactRecordInventoryLookup {
    selectors: BTreeMap<String, JsonValue>,
    explicit_gaps: BTreeMap<String, JsonValue>,
    unsupported_families: BTreeMap<String, String>,
}

pub(crate) fn parse_compact_name_records_request(
    query: &NameRecordsQuery,
    default_mode: CompactNameRecordsDefaultMode,
) -> ApiResult<CompactNameRecordsRequest> {
    let mode = parse_compact_name_records_mode(query.mode.as_deref(), default_mode.mode())?;
    let meta = parse_meta_mode(query.meta.as_deref(), MetaMode::Summary)?;
    let mut include = parse_compact_name_records_include(
        query.include.as_deref(),
        mode == CompactNameRecordsMode::Auto,
    )?;
    let known_text_keys =
        parse_compact_records_bool("known_text_keys", query.known_text_keys.as_deref())?
            || include.known_text_keys;
    let avatar =
        parse_compact_records_bool("avatar", query.avatar.as_deref())? || include.avatar;
    let content_hash = parse_compact_records_bool(
        "content_hash",
        query.content_hash.as_deref(),
    )? || include.content_hash;

    if known_text_keys {
        include.known_text_keys = true;
    }
    if avatar {
        include.avatar = true;
    }
    if content_hash {
        include.content_hash = true;
    }

    let coin_types = parse_compact_records_coin_types(query.coin_types.as_deref())?;
    if !coin_types.is_empty() {
        include.coins = true;
    }

    Ok(CompactNameRecordsRequest {
        mode,
        meta,
        texts: parse_compact_records_csv("texts", query.texts.as_deref())?,
        known_text_keys,
        avatar,
        content_hash,
        coin_types,
        include,
    })
}

pub(crate) fn compact_name_records_requested_records(
    record_inventory_row: Option<&RecordInventoryCurrentRow>,
    request: &CompactNameRecordsRequest,
) -> Vec<ResolutionRecordKey> {
    compact_requested_records(record_inventory_row, request)
}

pub(crate) fn compact_name_records_value_source(
    row: &NameCurrentRow,
    record_inventory_row: Option<&RecordInventoryCurrentRow>,
    requested_records: &[ResolutionRecordKey],
    request: &CompactNameRecordsRequest,
) -> CompactNameRecordsValueSource {
    match request.mode {
        CompactNameRecordsMode::Declared => CompactNameRecordsValueSource::Declared,
        CompactNameRecordsMode::Verified => CompactNameRecordsValueSource::Verified,
        CompactNameRecordsMode::Both => CompactNameRecordsValueSource::Verified,
        CompactNameRecordsMode::Auto => {
            if compact_declared_records_satisfy_request(
                row,
                record_inventory_row,
                requested_records,
            ) {
                CompactNameRecordsValueSource::Declared
            } else {
                CompactNameRecordsValueSource::Verified
            }
        }
    }
}

pub(crate) fn build_compact_name_records_response(
    row: &NameCurrentRow,
    record_inventory_row: Option<&RecordInventoryCurrentRow>,
    requested_records: &[ResolutionRecordKey],
    request: &CompactNameRecordsRequest,
    value_source: CompactNameRecordsValueSource,
    verified_outcome: Option<&ExecutionOutcome>,
) -> CompactNameRecordsResponse {
    let inventory_lookup = compact_record_inventory_lookup(record_inventory_row);
    let value_entries = match value_source {
        CompactNameRecordsValueSource::Declared => {
            compact_declared_record_cache_entries(row, record_inventory_row, requested_records)
        }
        CompactNameRecordsValueSource::Verified => {
            compact_verified_record_cache_entries(requested_records, verified_outcome)
        }
    };

    let mut data = empty_object();
    insert_value_field(
        &mut data,
        "resolver_address",
        if request.include.resolver_address {
            compact_resolver_address(row)
        } else {
            JsonValue::Null
        },
    );
    insert_value_field(
        &mut data,
        "text_records",
        compact_text_records(
            record_inventory_row,
            request,
            &value_entries,
            &inventory_lookup,
        ),
    );
    insert_value_field(
        &mut data,
        "known_text_keys",
        compact_known_text_keys(row, record_inventory_row, request, &inventory_lookup),
    );
    insert_value_field(
        &mut data,
        "avatar",
        compact_optional_record(
            request.avatar,
            "avatar",
            &value_entries,
            &inventory_lookup,
        ),
    );
    insert_value_field(
        &mut data,
        "content_hash",
        compact_optional_record(
            request.content_hash,
            "contenthash",
            &value_entries,
            &inventory_lookup,
        ),
    );
    insert_value_field(
        &mut data,
        "coin_addresses",
        compact_coin_addresses(
            request,
            record_inventory_row,
            &value_entries,
            &inventory_lookup,
        ),
    );
    if request.mode == CompactNameRecordsMode::Both {
        insert_value_field(
            &mut data,
            "verified_records",
            compact_verified_records_summary(requested_records, verified_outcome),
        );
    }

    let mut response = json!({ "data": data });
    if request.meta != MetaMode::None {
        insert_value_field(
            &mut response,
            "meta",
            compact_name_records_meta(
                row,
                record_inventory_row,
                request,
                value_source,
                verified_outcome,
            ),
        );
    }
    response
}

fn parse_compact_name_records_mode(
    mode: Option<&str>,
    default_mode: CompactNameRecordsMode,
) -> ApiResult<CompactNameRecordsMode> {
    let Some(mode) = mode.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(default_mode);
    };

    match mode {
        "auto" => Ok(CompactNameRecordsMode::Auto),
        "declared" => Ok(CompactNameRecordsMode::Declared),
        "verified" => Ok(CompactNameRecordsMode::Verified),
        "both" => Ok(CompactNameRecordsMode::Both),
        _ => Err(ApiError {
            status: StatusCode::BAD_REQUEST,
            code: "invalid_input",
            message: "mode must be one of: auto, declared, verified, both".to_owned(),
        }),
    }
}

fn parse_compact_records_bool(field_name: &str, value: Option<&str>) -> ApiResult<bool> {
    let Some(value) = value.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(false);
    };

    match value {
        "true" => Ok(true),
        "false" => Ok(false),
        _ => Err(ApiError {
            status: StatusCode::BAD_REQUEST,
            code: "invalid_input",
            message: format!("{field_name} must be true or false"),
        }),
    }
}

fn parse_compact_name_records_include(
    include: Option<&str>,
    include_all_by_default: bool,
) -> ApiResult<CompactNameRecordsInclude> {
    let mut parsed = if include_all_by_default {
        CompactNameRecordsInclude {
            resolver_address: true,
            known_text_keys: true,
            avatar: true,
            content_hash: true,
            coins: true,
        }
    } else {
        CompactNameRecordsInclude {
            resolver_address: true,
            ..CompactNameRecordsInclude::default()
        }
    };
    let Some(include) = include.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(parsed);
    };

    parsed = CompactNameRecordsInclude::default();
    for value in include.split(',').map(str::trim).filter(|value| !value.is_empty()) {
        match value {
            "resolver_address" => parsed.resolver_address = true,
            "known_text_keys" => parsed.known_text_keys = true,
            "avatar" => parsed.avatar = true,
            "content_hash" => parsed.content_hash = true,
            "coins" => parsed.coins = true,
            _ => {
                return Err(ApiError {
                    status: StatusCode::BAD_REQUEST,
                    code: "invalid_input",
                    message: "include must contain only resolver_address, known_text_keys, avatar, content_hash, or coins".to_owned(),
                });
            }
        }
    }

    Ok(parsed)
}

fn parse_compact_records_csv(field_name: &str, value: Option<&str>) -> ApiResult<Vec<String>> {
    let mut parsed = Vec::new();
    let mut deduped = BTreeSet::new();

    for item in value
        .unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        if item.chars().any(char::is_whitespace) {
            return Err(ApiError {
                status: StatusCode::BAD_REQUEST,
                code: "invalid_input",
                message: format!("{field_name} must be a comma-separated selector list"),
            });
        }
        if deduped.insert(item.to_owned()) {
            parsed.push(item.to_owned());
        }
    }

    Ok(parsed)
}

fn parse_compact_records_coin_types(value: Option<&str>) -> ApiResult<Vec<String>> {
    let coin_types = parse_compact_records_csv("coin_types", value)?;
    if coin_types
        .iter()
        .all(|coin_type| coin_type.as_bytes().iter().all(u8::is_ascii_digit))
    {
        Ok(coin_types)
    } else {
        Err(ApiError {
            status: StatusCode::BAD_REQUEST,
            code: "invalid_input",
            message: "coin_types must contain only decimal coin-type selectors".to_owned(),
        })
    }
}

include!("records_declared_inventory.rs");
include!("records_declared_values.rs");
include!("records_value_meta.rs");
