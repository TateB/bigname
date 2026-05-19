use super::{
    APP_RELATION_QUERY, ApiParameterSchema, ApiRouteParameter, CHAIN_ID_PATH,
    COMPACT_FULL_VIEW_QUERY, COMPACT_ONLY_VIEW_QUERY, CURSOR_QUERY, HISTORY_SCOPE_QUERY,
    HISTORY_VIEW_QUERY, NAME_PATH, NAMESPACE_PATH, NAMESPACE_QUERY, PAGE_SIZE_QUERY, RECORDS_QUERY,
    REQUIRED_NAMESPACE_QUERY, RESOLUTION_MODE_QUERY, RESOLVER_ADDRESS_PATH, RESOURCE_ID_PATH,
    SUMMARY_META_QUERY,
};

const RECORDS_MODE_DECLARED_QUERY: ApiRouteParameter = ApiRouteParameter::query(
    "mode",
    "Compact records read mode. `auto` uses declared cache when it can satisfy the requested values from replayable state, otherwise verified resolution for requested selectors. When no declared selectors are available, app-facing defaults probe only a bounded basic profile set.",
    ApiParameterSchema::StringEnumDefault {
        values: &["auto", "declared", "verified", "both"],
        default: "declared",
    },
);
const RECORDS_MODE_AUTO_QUERY: ApiRouteParameter = ApiRouteParameter::query(
    "mode",
    "Compact records read mode. `auto` uses declared cache when it can satisfy the requested values from replayable state, otherwise verified resolution for requested selectors. When no declared selectors are available, app-facing defaults probe only a bounded basic profile set.",
    ApiParameterSchema::StringEnumDefault {
        values: &["auto", "declared", "verified", "both"],
        default: "auto",
    },
);
const TEXTS_QUERY: ApiRouteParameter = ApiRouteParameter::csv_query(
    "texts",
    "Requested text record keys.",
    ApiParameterSchema::String,
);
const KNOWN_TEXT_KEYS_QUERY: ApiRouteParameter = ApiRouteParameter::query(
    "known_text_keys",
    "Whether to return projected known text-key inventory.",
    ApiParameterSchema::Boolean,
);
const AVATAR_QUERY: ApiRouteParameter = ApiRouteParameter::query(
    "avatar",
    "Whether to request the avatar text convenience field.",
    ApiParameterSchema::Boolean,
);
const CONTENT_HASH_QUERY: ApiRouteParameter = ApiRouteParameter::query(
    "content_hash",
    "Whether to request the content-hash selector.",
    ApiParameterSchema::Boolean,
);
const COIN_TYPES_QUERY: ApiRouteParameter = ApiRouteParameter::csv_query(
    "coin_types",
    "Requested textual coin-type selector keys.",
    ApiParameterSchema::String,
);
const RECORDS_INCLUDE_DECLARED_QUERY: ApiRouteParameter = ApiRouteParameter::csv_query(
    "include",
    "Optional compact record sections.",
    ApiParameterSchema::StringEnumDefault {
        values: &[
            "resolver_address",
            "known_text_keys",
            "avatar",
            "content_hash",
            "coins",
        ],
        default: "resolver_address",
    },
);
const RECORDS_INCLUDE_AUTO_QUERY: ApiRouteParameter = ApiRouteParameter::csv_query(
    "include",
    "Optional compact record sections.",
    ApiParameterSchema::StringEnumDefault {
        values: &[
            "resolver_address",
            "known_text_keys",
            "avatar",
            "content_hash",
            "coins",
        ],
        default: "resolver_address,known_text_keys,avatar,content_hash,coins",
    },
);

pub(crate) const NAME_RECORDS_PARAMETERS: &[ApiRouteParameter] = &[
    NAMESPACE_PATH,
    NAME_PATH,
    RECORDS_MODE_DECLARED_QUERY,
    TEXTS_QUERY,
    KNOWN_TEXT_KEYS_QUERY,
    AVATAR_QUERY,
    CONTENT_HASH_QUERY,
    COIN_TYPES_QUERY,
    RECORDS_INCLUDE_DECLARED_QUERY,
    COMPACT_ONLY_VIEW_QUERY,
    SUMMARY_META_QUERY,
];

pub(crate) const RESOLVE_RECORDS_PARAMETERS: &[ApiRouteParameter] = &[
    NAME_PATH,
    RECORDS_MODE_AUTO_QUERY,
    TEXTS_QUERY,
    KNOWN_TEXT_KEYS_QUERY,
    AVATAR_QUERY,
    CONTENT_HASH_QUERY,
    COIN_TYPES_QUERY,
    RECORDS_INCLUDE_AUTO_QUERY,
    COMPACT_ONLY_VIEW_QUERY,
    SUMMARY_META_QUERY,
];

pub(crate) const NAME_ROLES_PARAMETERS: &[ApiRouteParameter] = &[
    NAMESPACE_PATH,
    NAME_PATH,
    ApiRouteParameter::query(
        "account",
        "Effective permission subject filter.",
        ApiParameterSchema::String,
    ),
    ApiRouteParameter::query(
        "role_bitmap",
        "Projected role bitmap filter when supported.",
        ApiParameterSchema::String,
    ),
    COMPACT_ONLY_VIEW_QUERY,
    SUMMARY_META_QUERY,
    CURSOR_QUERY,
    PAGE_SIZE_QUERY,
];

pub(crate) const EVENTS_PARAMETERS: &[ApiRouteParameter] = &[
    NAMESPACE_QUERY,
    ApiRouteParameter::query(
        "name",
        "Normalized name event anchor filter.",
        ApiParameterSchema::String,
    ),
    ApiRouteParameter::query(
        "address",
        "Address relation event filter.",
        ApiParameterSchema::String,
    ),
    ApiRouteParameter::query(
        "resource",
        "Opaque resource identifier filter.",
        ApiParameterSchema::UuidString,
    ),
    ApiRouteParameter::query(
        "resource_id",
        "Opaque resource identifier filter.",
        ApiParameterSchema::UuidString,
    ),
    ApiRouteParameter::query(
        "type",
        "Normalized event type or compact type alias filter.",
        ApiParameterSchema::String,
    ),
    APP_RELATION_QUERY,
    ApiRouteParameter::query(
        "from_block",
        "Inclusive canonical block lower bound.",
        ApiParameterSchema::IntegerMin(0),
    ),
    ApiRouteParameter::query(
        "to_block",
        "Inclusive canonical block upper bound.",
        ApiParameterSchema::IntegerMin(0),
    ),
    COMPACT_ONLY_VIEW_QUERY,
    SUMMARY_META_QUERY,
    CURSOR_QUERY,
    PAGE_SIZE_QUERY,
];

pub(crate) const ROLES_PARAMETERS: &[ApiRouteParameter] = &[
    ApiRouteParameter::query(
        "account",
        "Effective permission subject filter.",
        ApiParameterSchema::String,
    ),
    ApiRouteParameter::query(
        "resource_id",
        "Opaque resource identifier filter.",
        ApiParameterSchema::UuidString,
    ),
    NAMESPACE_QUERY,
    ApiRouteParameter::query(
        "name",
        "Normalized name lookup filter paired with namespace.",
        ApiParameterSchema::String,
    ),
    ApiRouteParameter::query(
        "role_bitmap",
        "Projected role bitmap filter when supported.",
        ApiParameterSchema::String,
    ),
    COMPACT_ONLY_VIEW_QUERY,
    SUMMARY_META_QUERY,
    CURSOR_QUERY,
    PAGE_SIZE_QUERY,
];

pub(crate) const RESOURCE_LOOKUP_PARAMETERS: &[ApiRouteParameter] = &[
    REQUIRED_NAMESPACE_QUERY,
    ApiRouteParameter::required_query(
        "name",
        "Required normalized name to resolve to a current resource identity.",
        ApiParameterSchema::String,
    ),
    COMPACT_ONLY_VIEW_QUERY,
    SUMMARY_META_QUERY,
];

pub(crate) const RESOLVE_CURRENT_PARAMETERS: &[ApiRouteParameter] =
    &[NAME_PATH, RESOLUTION_MODE_QUERY, RECORDS_QUERY];

pub(crate) const RESOLUTION_CURRENT_PARAMETERS: &[ApiRouteParameter] = &[
    NAMESPACE_PATH,
    NAME_PATH,
    ApiRouteParameter::query(
        "at",
        "Point-in-time selector for the exact-name snapshot used by resolution joins. Mutually exclusive with `chain_positions`.",
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
    RESOLUTION_MODE_QUERY,
    RECORDS_QUERY,
];

pub(crate) const RESOLVER_PATH_PARAMETERS: &[ApiRouteParameter] =
    &[CHAIN_ID_PATH, RESOLVER_ADDRESS_PATH];

pub(crate) const RESOLVER_OVERVIEW_PARAMETERS: &[ApiRouteParameter] = &[
    CHAIN_ID_PATH,
    RESOLVER_ADDRESS_PATH,
    ApiRouteParameter::csv_query(
        "include",
        "Requested compact resolver overview sections.",
        ApiParameterSchema::StringEnumDefault {
            values: &["nodes", "aliases", "roles", "events"],
            default: "nodes,aliases,roles,events",
        },
    ),
    COMPACT_FULL_VIEW_QUERY,
    SUMMARY_META_QUERY,
];

pub(crate) const NAME_HISTORY_PARAMETERS: &[ApiRouteParameter] = &[
    NAMESPACE_PATH,
    NAME_PATH,
    HISTORY_SCOPE_QUERY,
    HISTORY_VIEW_QUERY,
    SUMMARY_META_QUERY,
    CURSOR_QUERY,
    PAGE_SIZE_QUERY,
];

pub(crate) const RESOURCE_HISTORY_PARAMETERS: &[ApiRouteParameter] = &[
    RESOURCE_ID_PATH,
    HISTORY_SCOPE_QUERY,
    HISTORY_VIEW_QUERY,
    SUMMARY_META_QUERY,
    CURSOR_QUERY,
    PAGE_SIZE_QUERY,
];

pub(crate) const RESOURCE_PERMISSIONS_PARAMETERS: &[ApiRouteParameter] = &[
    RESOURCE_ID_PATH,
    ApiRouteParameter::query(
        "subject",
        "Optional subject filter for the current effective permissions rows.",
        ApiParameterSchema::String,
    ),
    ApiRouteParameter::query(
        "scope",
        "Optional scope filter. Accepts `root`, `registry`, `resource`, `resolver:{chain_id}:{resolver_address}`, `record_manager:{chain_id}:{manager_address}`, `migration_derived:{resource_id}`, or `transport_derived:{transport}`.",
        ApiParameterSchema::String,
    ),
    CURSOR_QUERY,
    PAGE_SIZE_QUERY,
];
