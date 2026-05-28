#[path = "parameters/app.rs"]
mod app;

pub(crate) use app::*;

#[derive(Clone, Copy)]
pub(crate) struct ApiRouteParameter {
    pub(crate) name: &'static str,
    pub(crate) location: ApiParameterLocation,
    pub(crate) required: bool,
    pub(crate) description: &'static str,
    pub(crate) schema: ApiParameterSchema,
    pub(crate) csv: bool,
}

#[derive(Clone, Copy)]
pub(crate) enum ApiParameterLocation {
    Path,
    Query,
}

#[derive(Clone, Copy)]
pub(crate) enum ApiParameterSchema {
    String,
    Boolean,
    UuidString,
    StringEnum(&'static [&'static str]),
    StringDefault(&'static str),
    StringPatternDefault {
        pattern: &'static str,
        default: &'static str,
    },
    StringEnumDefault {
        values: &'static [&'static str],
        default: &'static str,
    },
    IntegerMin(i64),
    IntegerRange {
        minimum: i64,
        maximum: u64,
    },
}

impl ApiRouteParameter {
    const fn path(
        name: &'static str,
        description: &'static str,
        schema: ApiParameterSchema,
    ) -> Self {
        Self {
            name,
            location: ApiParameterLocation::Path,
            required: true,
            description,
            schema,
            csv: false,
        }
    }

    const fn query(
        name: &'static str,
        description: &'static str,
        schema: ApiParameterSchema,
    ) -> Self {
        Self {
            name,
            location: ApiParameterLocation::Query,
            required: false,
            description,
            schema,
            csv: false,
        }
    }

    const fn required_query(
        name: &'static str,
        description: &'static str,
        schema: ApiParameterSchema,
    ) -> Self {
        Self {
            name,
            location: ApiParameterLocation::Query,
            required: true,
            description,
            schema,
            csv: false,
        }
    }

    const fn csv_query(
        name: &'static str,
        description: &'static str,
        schema: ApiParameterSchema,
    ) -> Self {
        Self {
            name,
            location: ApiParameterLocation::Query,
            required: false,
            description,
            schema,
            csv: true,
        }
    }

    const fn required_csv_query(
        name: &'static str,
        description: &'static str,
        schema: ApiParameterSchema,
    ) -> Self {
        Self {
            name,
            location: ApiParameterLocation::Query,
            required: true,
            description,
            schema,
            csv: true,
        }
    }
}

const NAMESPACE_PATH: ApiRouteParameter = ApiRouteParameter::path(
    "namespace",
    "Supported namespace identifier.",
    ApiParameterSchema::StringEnum(crate::PUBLIC_NAMESPACES),
);
const NAME_PATH: ApiRouteParameter = ApiRouteParameter::path(
    "name",
    "Normalized name within the namespace.",
    ApiParameterSchema::String,
);
const INFERRED_NAME_PATH: ApiRouteParameter = ApiRouteParameter::path(
    "name",
    "Name input normalized before namespace inference.",
    ApiParameterSchema::String,
);
const ADDRESS_PATH: ApiRouteParameter = ApiRouteParameter::path(
    "address",
    "Address anchor for the collection or history read. Addresses are normalized to lowercase.",
    ApiParameterSchema::String,
);
const RESOURCE_ID_PATH: ApiRouteParameter = ApiRouteParameter::path(
    "resource_id",
    "Resource identifier anchor.",
    ApiParameterSchema::UuidString,
);
const CHAIN_ID_PATH: ApiRouteParameter = ApiRouteParameter::path(
    "chain_id",
    "Resolver chain identifier.",
    ApiParameterSchema::String,
);
const RESOLVER_ADDRESS_PATH: ApiRouteParameter = ApiRouteParameter::path(
    "resolver_address",
    "Resolver address anchor. Addresses are normalized to lowercase.",
    ApiParameterSchema::String,
);
const NAMESPACE_QUERY: ApiRouteParameter = ApiRouteParameter::query(
    "namespace",
    "Optional namespace filter.",
    ApiParameterSchema::StringEnum(crate::PUBLIC_NAMESPACES),
);
const REQUIRED_NAMESPACE_QUERY: ApiRouteParameter = ApiRouteParameter::required_query(
    "namespace",
    "Required namespace identifier.",
    ApiParameterSchema::StringEnum(crate::PUBLIC_NAMESPACES),
);
const PRIMARY_NAMESPACE_QUERY: ApiRouteParameter = ApiRouteParameter::query(
    "namespace",
    "Primary-name namespace. Defaults to ENS for the app fast path.",
    ApiParameterSchema::StringEnumDefault {
        values: crate::PUBLIC_NAMESPACES,
        default: "ens",
    },
);
const RELATION_QUERY: ApiRouteParameter = ApiRouteParameter::query(
    "relation",
    "Optional relation facet filter.",
    ApiParameterSchema::StringEnum(&["registrant", "token_holder", "effective_controller"]),
);
const APP_RELATION_QUERY: ApiRouteParameter = ApiRouteParameter::query(
    "relation",
    "Optional app-facing relation facet filter.",
    ApiParameterSchema::StringEnum(&["token_holder", "registrant", "effective_controller", "any"]),
);
const DEDUPE_BY_QUERY: ApiRouteParameter = ApiRouteParameter::query(
    "dedupe_by",
    "Current collection dedupe basis.",
    ApiParameterSchema::StringEnumDefault {
        values: &["surface", "resource"],
        default: "surface",
    },
);
const HISTORY_SCOPE_QUERY: ApiRouteParameter = ApiRouteParameter::query(
    "scope",
    "History scope selector.",
    ApiParameterSchema::StringEnumDefault {
        values: &["surface", "resource", "both"],
        default: "both",
    },
);
const HISTORY_VIEW_QUERY: ApiRouteParameter = ApiRouteParameter::query(
    "view",
    "Response view selector.",
    ApiParameterSchema::StringEnumDefault {
        values: &["compact", "full"],
        default: "compact",
    },
);
const COMPACT_FULL_VIEW_QUERY: ApiRouteParameter = ApiRouteParameter::query(
    "view",
    "Response view selector.",
    ApiParameterSchema::StringEnumDefault {
        values: &["compact", "full"],
        default: "compact",
    },
);
const COMPACT_ONLY_VIEW_QUERY: ApiRouteParameter = ApiRouteParameter::query(
    "view",
    "Compact response view selector. `view=full` remains a compatibility-reserved value and returns `400 invalid_input`.",
    ApiParameterSchema::StringEnumDefault {
        values: &["compact"],
        default: "compact",
    },
);
const SUMMARY_META_QUERY: ApiRouteParameter = ApiRouteParameter::query(
    "meta",
    "Compact response metadata selector.",
    ApiParameterSchema::StringEnumDefault {
        values: &["none", "summary", "full"],
        default: "summary",
    },
);
const CURSOR_QUERY: ApiRouteParameter = ApiRouteParameter::query(
    "cursor",
    "Replay-stable pagination cursor.",
    ApiParameterSchema::String,
);
const PAGE_SIZE_QUERY: ApiRouteParameter = ApiRouteParameter::query(
    "page_size",
    "Optional page size. When supplied it must be between 1 and 200.",
    ApiParameterSchema::IntegerRange {
        minimum: 1,
        maximum: crate::MAX_PAGE_SIZE,
    },
);
const PRIMARY_NAME_MODE_QUERY: ApiRouteParameter = ApiRouteParameter::query(
    "mode",
    "Primary-name read mode.",
    ApiParameterSchema::StringEnumDefault {
        values: &["declared", "verified", "both"],
        default: "declared",
    },
);
const ORDER_QUERY: ApiRouteParameter = ApiRouteParameter::query(
    "order",
    "Stable sort order.",
    ApiParameterSchema::StringEnumDefault {
        values: &["asc", "desc"],
        default: "asc",
    },
);

pub(crate) const NAMES_PARAMETERS: &[ApiRouteParameter] = &[
    NAMESPACE_QUERY,
    ApiRouteParameter::query(
        "name",
        "Exact normalized-name lookup filter.",
        ApiParameterSchema::String,
    ),
    ApiRouteParameter::query(
        "prefix",
        "Normalized-name prefix search filter.",
        ApiParameterSchema::String,
    ),
    ApiRouteParameter::query(
        "contains",
        "Normalized-name contains search filter.",
        ApiParameterSchema::String,
    ),
    ApiRouteParameter::query(
        "contains_nocase",
        "Case-insensitive normalized-name contains search filter.",
        ApiParameterSchema::String,
    ),
    ApiRouteParameter::query(
        "owner",
        "Token-holder / owner address filter.",
        ApiParameterSchema::String,
    ),
    ApiRouteParameter::query(
        "account",
        "Address relation filter anchor.",
        ApiParameterSchema::String,
    ),
    ApiRouteParameter::query(
        "registrant",
        "Registrant address filter.",
        ApiParameterSchema::String,
    ),
    ApiRouteParameter::query(
        "resolver",
        "Current declared resolver address filter.",
        ApiParameterSchema::String,
    ),
    ApiRouteParameter::query(
        "resolved_address",
        "Declared record-value equality filter when projected.",
        ApiParameterSchema::String,
    ),
    APP_RELATION_QUERY,
    ApiRouteParameter::query(
        "sort",
        "Stable compact names sort key.",
        ApiParameterSchema::StringEnumDefault {
            values: &["name", "expiry_date", "registration_date", "created_at"],
            default: "name",
        },
    ),
    ORDER_QUERY,
    ApiRouteParameter::csv_query(
        "include",
        "Optional compact name expansions.",
        ApiParameterSchema::StringEnum(&["record_summaries", "total_count"]),
    ),
    COMPACT_ONLY_VIEW_QUERY,
    SUMMARY_META_QUERY,
    CURSOR_QUERY,
    PAGE_SIZE_QUERY,
];

pub(crate) const ADDRESS_NAMES_PARAMETERS: &[ApiRouteParameter] = &[
    ADDRESS_PATH,
    NAMESPACE_QUERY,
    RELATION_QUERY,
    DEDUPE_BY_QUERY,
    ApiRouteParameter::csv_query(
        "include",
        "Optional collection expansions. `role_summary` is the only shipped expansion.",
        ApiParameterSchema::StringEnum(&["role_summary"]),
    ),
    CURSOR_QUERY,
    PAGE_SIZE_QUERY,
];

pub(crate) const ADDRESS_HISTORY_PARAMETERS: &[ApiRouteParameter] = &[
    ADDRESS_PATH,
    NAMESPACE_QUERY,
    RELATION_QUERY,
    HISTORY_SCOPE_QUERY,
    HISTORY_VIEW_QUERY,
    SUMMARY_META_QUERY,
    CURSOR_QUERY,
    PAGE_SIZE_QUERY,
];

pub(crate) const PRIMARY_NAMES_PARAMETERS: &[ApiRouteParameter] = &[
    ADDRESS_PATH,
    PRIMARY_NAMESPACE_QUERY,
    ApiRouteParameter::query(
        "coin_type",
        "`coin_type` selector. Defaults to 60 for the app fast path.",
        ApiParameterSchema::StringPatternDefault {
            pattern: "^[0-9]+$",
            default: "60",
        },
    ),
    PRIMARY_NAME_MODE_QUERY,
];

pub(crate) const EXACT_NAME_SNAPSHOT_PARAMETERS: &[ApiRouteParameter] = &[
    NAMESPACE_PATH,
    NAME_PATH,
    ApiRouteParameter::query(
        "at",
        "Point-in-time selector for the exact-name snapshot. Mutually exclusive with `chain_positions`.",
        ApiParameterSchema::String,
    ),
    ApiRouteParameter::query(
        "chain_positions",
        "Explicit exact-name snapshot selector encoded as one JSON object using ChainPositions position objects. Mutually exclusive with `at`.",
        ApiParameterSchema::String,
    ),
    ApiRouteParameter::query(
        "consistency",
        "Snapshot consistency floor. Defaults to `head`.",
        ApiParameterSchema::StringEnumDefault {
            values: &["head", "safe", "finalized"],
            default: "head",
        },
    ),
];

pub(crate) const EXPLAIN_RESOLUTION_EXECUTION_PARAMETERS: &[ApiRouteParameter] = &[
    NAMESPACE_PATH,
    NAME_PATH,
    ApiRouteParameter::required_csv_query(
        "records",
        "Comma-separated record selectors. Required for the persisted execution explain lookup.",
        ApiParameterSchema::String,
    ),
];

pub(crate) const NAMESPACE_PATH_PARAMETERS: &[ApiRouteParameter] = &[NAMESPACE_PATH];

pub(crate) const NAME_CHILDREN_PARAMETERS: &[ApiRouteParameter] = &[
    NAMESPACE_PATH,
    NAME_PATH,
    ApiRouteParameter::csv_query(
        "surface_classes",
        "Requested child surface classes. Only `declared` is currently supported.",
        ApiParameterSchema::StringDefault("declared"),
    ),
    ApiRouteParameter::csv_query(
        "include",
        "Optional collection expansions. `counts` includes compact row `subname_count` or full-envelope `declared_state.subname_count`.",
        ApiParameterSchema::StringEnum(&["counts"]),
    ),
    COMPACT_FULL_VIEW_QUERY,
    SUMMARY_META_QUERY,
    CURSOR_QUERY,
    PAGE_SIZE_QUERY,
];
