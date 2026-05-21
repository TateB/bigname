use serde::Deserialize;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ResponseView {
    Compact,
    Full,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum MetaMode {
    None,
    Summary,
    Full,
}

#[derive(Clone, Debug, Default, Deserialize)]
pub(crate) struct HistoryQuery {
    pub(crate) scope: Option<String>,
    pub(crate) view: Option<String>,
    pub(crate) meta: Option<String>,
    pub(crate) cursor: Option<String>,
    pub(crate) page_size: Option<u64>,
}

#[derive(Clone, Debug, Default, Deserialize)]
pub(crate) struct PermissionsQuery {
    pub(crate) subject: Option<String>,
    pub(crate) scope: Option<String>,
    pub(crate) cursor: Option<String>,
    pub(crate) page_size: Option<u64>,
}

#[derive(Clone, Debug, Default, Deserialize)]
pub(crate) struct ChildrenQuery {
    pub(crate) surface_classes: Option<String>,
    pub(crate) include: Option<String>,
    pub(crate) view: Option<String>,
    pub(crate) meta: Option<String>,
    pub(crate) cursor: Option<String>,
    pub(crate) page_size: Option<u64>,
}

#[derive(Clone, Debug, Default, Deserialize)]
pub(crate) struct AddressNamesQuery {
    pub(crate) namespace: Option<String>,
    pub(crate) relation: Option<String>,
    pub(crate) dedupe_by: Option<String>,
    pub(crate) include: Option<String>,
    pub(crate) cursor: Option<String>,
    pub(crate) page_size: Option<u64>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct AddressNamesIncludeOptions {
    pub(crate) role_summary: bool,
}

#[derive(Clone, Debug, Default, Deserialize)]
pub(crate) struct NamesQuery {
    pub(crate) namespace: Option<String>,
    pub(crate) name: Option<String>,
    pub(crate) prefix: Option<String>,
    pub(crate) contains: Option<String>,
    pub(crate) contains_nocase: Option<String>,
    pub(crate) owner: Option<String>,
    pub(crate) account: Option<String>,
    pub(crate) registrant: Option<String>,
    pub(crate) resolver: Option<String>,
    pub(crate) resolved_address: Option<String>,
    pub(crate) relation: Option<String>,
    pub(crate) sort: Option<String>,
    pub(crate) order: Option<String>,
    pub(crate) include: Option<String>,
    pub(crate) view: Option<String>,
    pub(crate) meta: Option<String>,
    pub(crate) cursor: Option<String>,
    pub(crate) page_size: Option<u64>,
}

#[derive(Clone, Debug, Default, Deserialize)]
pub(crate) struct AddressNamesCountQuery {
    pub(crate) namespace: Option<String>,
    pub(crate) relation: Option<String>,
    pub(crate) prefix: Option<String>,
    pub(crate) contains: Option<String>,
    pub(crate) contains_nocase: Option<String>,
    pub(crate) resolver: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize)]
pub(crate) struct AddressHistoryQuery {
    pub(crate) namespace: Option<String>,
    pub(crate) relation: Option<String>,
    pub(crate) scope: Option<String>,
    pub(crate) view: Option<String>,
    pub(crate) meta: Option<String>,
    pub(crate) cursor: Option<String>,
    pub(crate) page_size: Option<u64>,
}

#[derive(Clone, Debug, Default, Deserialize)]
pub(crate) struct ExactNameSnapshotQuery {
    pub(crate) at: Option<String>,
    pub(crate) chain_positions: Option<String>,
    pub(crate) consistency: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize)]
pub(crate) struct ResolutionQuery {
    pub(crate) at: Option<String>,
    pub(crate) chain_positions: Option<String>,
    pub(crate) consistency: Option<String>,
    pub(crate) mode: Option<String>,
    pub(crate) records: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize)]
pub(crate) struct InferredResolutionQuery {
    pub(crate) mode: Option<String>,
    pub(crate) records: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize)]
pub(crate) struct ResolutionExecutionExplainQuery {
    pub(crate) records: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize)]
pub(crate) struct NameRecordsQuery {
    pub(crate) mode: Option<String>,
    pub(crate) texts: Option<String>,
    pub(crate) known_text_keys: Option<String>,
    pub(crate) avatar: Option<String>,
    pub(crate) content_hash: Option<String>,
    pub(crate) coin_types: Option<String>,
    pub(crate) include: Option<String>,
    pub(crate) view: Option<String>,
    pub(crate) meta: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize)]
pub(crate) struct PrimaryNameQuery {
    pub(crate) namespace: Option<String>,
    pub(crate) coin_type: Option<String>,
    pub(crate) mode: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize)]
pub(crate) struct EventsQuery {
    pub(crate) namespace: Option<String>,
    pub(crate) name: Option<String>,
    pub(crate) address: Option<String>,
    pub(crate) resource: Option<String>,
    pub(crate) resource_id: Option<String>,
    pub(crate) resource_hex: Option<String>,
    #[serde(rename = "type")]
    pub(crate) event_type: Option<String>,
    pub(crate) relation: Option<String>,
    pub(crate) selector: Option<String>,
    pub(crate) selector_key: Option<String>,
    pub(crate) record: Option<String>,
    pub(crate) record_key: Option<String>,
    pub(crate) records: Option<String>,
    pub(crate) texts: Option<String>,
    pub(crate) text_key: Option<String>,
    pub(crate) coin_type: Option<String>,
    pub(crate) coin_types: Option<String>,
    pub(crate) avatar: Option<String>,
    pub(crate) content_hash: Option<String>,
    pub(crate) from_block: Option<String>,
    pub(crate) to_block: Option<String>,
    pub(crate) view: Option<String>,
    pub(crate) meta: Option<String>,
    pub(crate) cursor: Option<String>,
    pub(crate) page_size: Option<u64>,
}

#[derive(Clone, Debug, Default, Deserialize)]
pub(crate) struct RolesQuery {
    pub(crate) account: Option<String>,
    pub(crate) resource_id: Option<String>,
    pub(crate) namespace: Option<String>,
    pub(crate) name: Option<String>,
    pub(crate) role_bitmap: Option<String>,
    pub(crate) view: Option<String>,
    pub(crate) meta: Option<String>,
    pub(crate) cursor: Option<String>,
    pub(crate) page_size: Option<u64>,
}

#[derive(Clone, Debug, Default, Deserialize)]
pub(crate) struct NameRolesQuery {
    pub(crate) account: Option<String>,
    pub(crate) role_bitmap: Option<String>,
    pub(crate) view: Option<String>,
    pub(crate) meta: Option<String>,
    pub(crate) cursor: Option<String>,
    pub(crate) page_size: Option<u64>,
}

#[derive(Clone, Debug, Default, Deserialize)]
pub(crate) struct ResourceLookupQuery {
    pub(crate) namespace: Option<String>,
    pub(crate) name: Option<String>,
    pub(crate) view: Option<String>,
    pub(crate) meta: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize)]
pub(crate) struct ResolverOverviewQuery {
    pub(crate) include: Option<String>,
    pub(crate) view: Option<String>,
    pub(crate) meta: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ResolutionMode {
    Declared,
    Verified,
    Both,
}

impl ResolutionMode {
    pub(crate) fn includes_declared(self) -> bool {
        matches!(self, Self::Declared | Self::Both)
    }

    pub(crate) fn includes_verified(self) -> bool {
        matches!(self, Self::Verified | Self::Both)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ResolutionRecordKey {
    pub(crate) record_key: String,
    pub(crate) record_family: String,
    pub(crate) selector_key: Option<String>,
}
