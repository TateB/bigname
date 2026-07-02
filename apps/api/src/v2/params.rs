use serde::Deserialize;
use sqlx::types::Uuid;

use super::{
    error::{V2Error, V2Result},
    vocab::{
        AddressNamesDedupe, AddressNamesSort, Finality, HistoryEventType, HistoryScope, Relation,
        RelationSet,
    },
};

pub(crate) const DEFAULT_PAGE_SIZE: u64 = 50;
pub(crate) const MAX_PAGE_SIZE: u64 = 200;

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq)]
pub(crate) struct RawQueryParams {
    pub(crate) at: Option<String>,
    pub(crate) finality: Option<String>,
    pub(crate) source: Option<String>,
    pub(crate) coin_type: Option<String>,
    pub(crate) keys: Option<String>,
    pub(crate) namespace: Option<String>,
    pub(crate) include: Option<String>,
    pub(crate) scope: Option<String>,
    #[serde(rename = "type")]
    pub(crate) event_type: Option<String>,
    pub(crate) name: Option<String>,
    pub(crate) registration_id: Option<String>,
    pub(crate) address: Option<String>,
    pub(crate) relation: Option<String>,
    pub(crate) from_block: Option<String>,
    pub(crate) to_block: Option<String>,
    pub(crate) q: Option<String>,
    pub(crate) dedupe: Option<String>,
    pub(crate) sort: Option<String>,
    pub(crate) order: Option<String>,
    pub(crate) cursor: Option<String>,
    pub(crate) page_size: Option<u64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct QueryParams {
    pub(crate) at: Option<AtSelector>,
    pub(crate) finality: Finality,
    pub(crate) source: RequestSource,
    pub(crate) coin_type: Option<String>,
    pub(crate) keys: Option<String>,
    pub(crate) namespace: Option<String>,
    pub(crate) include: Vec<String>,
    pub(crate) scope: HistoryScope,
    pub(crate) event_type: Option<HistoryEventType>,
    pub(crate) name: Option<String>,
    pub(crate) registration_id: Option<String>,
    pub(crate) address: Option<String>,
    pub(crate) relation: Option<RelationSet>,
    pub(crate) from_block: Option<i64>,
    pub(crate) to_block: Option<i64>,
    pub(crate) q: Option<String>,
    pub(crate) dedupe: AddressNamesDedupe,
    pub(crate) sort: AddressNamesSort,
    pub(crate) order: SortOrder,
    pub(crate) cursor: Option<String>,
    pub(crate) page_size: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum AtSelector {
    Timestamp(String),
    SnapshotToken(String),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RequestSource {
    Indexed,
    Verified,
    Auto,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SortOrder {
    Asc,
    Desc,
}

impl TryFrom<RawQueryParams> for QueryParams {
    type Error = V2Error;

    fn try_from(raw: RawQueryParams) -> Result<Self, Self::Error> {
        Ok(Self {
            at: raw.at.as_deref().map(parse_at).transpose()?,
            finality: parse_finality(raw.finality.as_deref())?,
            source: parse_source(raw.source.as_deref())?,
            coin_type: trim_to_option(raw.coin_type),
            keys: trim_to_option(raw.keys),
            namespace: trim_to_option(raw.namespace),
            include: parse_include(raw.include),
            scope: parse_scope(raw.scope.as_deref())?,
            event_type: parse_event_type(raw.event_type.as_deref())?,
            name: trim_to_option(raw.name),
            registration_id: parse_registration_id(raw.registration_id)?,
            address: parse_address(raw.address)?,
            relation: parse_relation_set_param(raw.relation.as_deref())?,
            from_block: parse_block_bound(raw.from_block, "from_block")?,
            to_block: parse_block_bound(raw.to_block, "to_block")?,
            q: trim_to_option(raw.q),
            dedupe: parse_dedupe(raw.dedupe.as_deref())?,
            sort: parse_sort(raw.sort.as_deref())?,
            order: parse_order(raw.order.as_deref())?,
            cursor: trim_to_option(raw.cursor),
            page_size: parse_page_size(raw.page_size)?,
        })
    }
}

fn parse_at(value: &str) -> V2Result<AtSelector> {
    let value = value.trim();
    if value.is_empty() {
        return Err(invalid_parameter("at"));
    }

    if bigname_storage::parse_rfc3339_utc_timestamp(value).is_ok() {
        return Ok(AtSelector::Timestamp(value.to_owned()));
    }

    if is_url_safe_opaque_token(value) {
        return Ok(AtSelector::SnapshotToken(value.to_owned()));
    }

    Err(invalid_parameter("at"))
}

fn parse_finality(value: Option<&str>) -> V2Result<Finality> {
    match value.map(str::trim).filter(|value| !value.is_empty()) {
        None | Some("latest") => Ok(Finality::Latest),
        Some("safe") => Ok(Finality::Safe),
        Some("finalized") => Ok(Finality::Finalized),
        Some(_) => Err(invalid_parameter("finality")),
    }
}

fn parse_source(value: Option<&str>) -> V2Result<RequestSource> {
    match value.map(str::trim).filter(|value| !value.is_empty()) {
        None | Some("indexed") => Ok(RequestSource::Indexed),
        Some("verified") => Ok(RequestSource::Verified),
        Some("auto") => Ok(RequestSource::Auto),
        Some(_) => Err(invalid_parameter("source")),
    }
}

fn parse_order(value: Option<&str>) -> V2Result<SortOrder> {
    match value.map(str::trim).filter(|value| !value.is_empty()) {
        None | Some("asc") => Ok(SortOrder::Asc),
        Some("desc") => Ok(SortOrder::Desc),
        Some(_) => Err(invalid_parameter("order")),
    }
}

impl SortOrder {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Asc => "asc",
            Self::Desc => "desc",
        }
    }
}

fn parse_scope(value: Option<&str>) -> V2Result<HistoryScope> {
    match value.map(str::trim).filter(|value| !value.is_empty()) {
        None | Some("both") => Ok(HistoryScope::Both),
        Some("name") => Ok(HistoryScope::Name),
        Some("registration") => Ok(HistoryScope::Registration),
        Some(_) => Err(invalid_parameter("scope")),
    }
}

fn parse_event_type(value: Option<&str>) -> V2Result<Option<HistoryEventType>> {
    match value.map(str::trim).filter(|value| !value.is_empty()) {
        None => Ok(None),
        Some("registration") => Ok(Some(HistoryEventType::Registration)),
        Some("renewal") => Ok(Some(HistoryEventType::Renewal)),
        Some("release") => Ok(Some(HistoryEventType::Release)),
        Some("expiry") => Ok(Some(HistoryEventType::Expiry)),
        Some("transfer") => Ok(Some(HistoryEventType::Transfer)),
        Some("authority") => Ok(Some(HistoryEventType::Authority)),
        Some("resolver") => Ok(Some(HistoryEventType::Resolver)),
        Some("record") => Ok(Some(HistoryEventType::Record)),
        Some("primary_name") => Ok(Some(HistoryEventType::PrimaryName)),
        Some("permission") => Ok(Some(HistoryEventType::Permission)),
        Some(_) => Err(invalid_parameter("type")),
    }
}

pub(crate) fn parse_relation_set_param(value: Option<&str>) -> V2Result<Option<RelationSet>> {
    let Some(value) = value.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(None);
    };

    let mut has_any = false;
    let mut relations = Vec::new();
    for part in value
        .split(',')
        .map(str::trim)
        .filter(|part| !part.is_empty())
    {
        if part == "any" {
            has_any = true;
            continue;
        }
        let Some(relation) = Relation::from_wire(part) else {
            return Err(invalid_parameter("relation"));
        };
        relations.push(relation);
    }

    if has_any {
        return Ok(Some(RelationSet::all()));
    }

    RelationSet::from_relations(relations)
        .map(Some)
        .ok_or_else(|| invalid_parameter("relation"))
}

fn parse_registration_id(value: Option<String>) -> V2Result<Option<String>> {
    let Some(value) = trim_to_option(value) else {
        return Ok(None);
    };

    Uuid::parse_str(&value)
        .map(|uuid| Some(uuid.to_string()))
        .map_err(|_| V2Error::invalid_input("registration_id must be a UUID"))
}

fn parse_address(value: Option<String>) -> V2Result<Option<String>> {
    let Some(value) = trim_to_option(value) else {
        return Ok(None);
    };

    crate::parse_evm_address(&value, "address")
        .map(Some)
        .map_err(|error| V2Error::invalid_input(error.message))
}

fn parse_block_bound(value: Option<String>, field_name: &'static str) -> V2Result<Option<i64>> {
    let Some(value) = trim_to_option(value) else {
        return Ok(None);
    };

    value
        .parse::<i64>()
        .ok()
        .filter(|value| *value >= 0)
        .map(Some)
        .ok_or_else(|| {
            V2Error::invalid_input(format!("{field_name} must be a non-negative integer"))
        })
}

fn parse_dedupe(value: Option<&str>) -> V2Result<AddressNamesDedupe> {
    match value.map(str::trim).filter(|value| !value.is_empty()) {
        None | Some("name") => Ok(AddressNamesDedupe::Name),
        Some("registration") => Ok(AddressNamesDedupe::Registration),
        Some(_) => Err(invalid_parameter("dedupe")),
    }
}

fn parse_sort(value: Option<&str>) -> V2Result<AddressNamesSort> {
    match value.map(str::trim).filter(|value| !value.is_empty()) {
        None | Some("name") => Ok(AddressNamesSort::Name),
        Some("expires_at") => Ok(AddressNamesSort::ExpiresAt),
        Some("registered_at") => Ok(AddressNamesSort::RegisteredAt),
        Some(_) => Err(invalid_parameter("sort")),
    }
}

fn parse_page_size(value: Option<u64>) -> V2Result<u64> {
    match value {
        None => Ok(DEFAULT_PAGE_SIZE),
        Some(value @ 1..=MAX_PAGE_SIZE) => Ok(value),
        Some(_) => Err(V2Error::invalid_input(format!(
            "page_size must be between 1 and {MAX_PAGE_SIZE}"
        ))),
    }
}

fn parse_include(value: Option<String>) -> Vec<String> {
    value
        .map(|value| {
            value
                .split(',')
                .map(str::trim)
                .filter(|part| !part.is_empty())
                .map(str::to_owned)
                .collect()
        })
        .unwrap_or_default()
}

fn trim_to_option(value: Option<String>) -> Option<String> {
    value
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

fn is_url_safe_opaque_token(value: &str) -> bool {
    value.bytes().all(|byte| {
        matches!(
            byte,
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~'
        )
    })
}

fn invalid_parameter(parameter: &'static str) -> V2Error {
    V2Error::invalid_input(format!("{parameter} is invalid"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::v2::error::ErrorCode;

    fn parse(raw: RawQueryParams) -> V2Result<QueryParams> {
        QueryParams::try_from(raw)
    }

    #[test]
    fn page_size_above_max_is_rejected() {
        let error = parse(RawQueryParams {
            page_size: Some(MAX_PAGE_SIZE + 1),
            ..RawQueryParams::default()
        })
        .expect_err("oversized page must fail");

        assert_eq!(error.code(), ErrorCode::InvalidInput);
    }

    #[test]
    fn bad_finality_source_and_order_are_rejected() {
        for raw in [
            RawQueryParams {
                finality: Some("pending".to_owned()),
                ..RawQueryParams::default()
            },
            RawQueryParams {
                source: Some("both".to_owned()),
                ..RawQueryParams::default()
            },
            RawQueryParams {
                order: Some("sideways".to_owned()),
                ..RawQueryParams::default()
            },
            RawQueryParams {
                relation: Some("token_holder".to_owned()),
                ..RawQueryParams::default()
            },
            RawQueryParams {
                dedupe: Some("surface".to_owned()),
                ..RawQueryParams::default()
            },
            RawQueryParams {
                sort: Some("expiry_date".to_owned()),
                ..RawQueryParams::default()
            },
            RawQueryParams {
                scope: Some("surface".to_owned()),
                ..RawQueryParams::default()
            },
        ] {
            let error = parse(raw).expect_err("bad enum value must fail");
            assert_eq!(error.code(), ErrorCode::InvalidInput);
        }
    }

    #[test]
    fn address_name_controls_default_and_parse_wire_values() {
        let defaulted = parse(RawQueryParams::default()).expect("default query must parse");
        assert_eq!(defaulted.relation, None);
        assert_eq!(defaulted.q, None);
        assert_eq!(defaulted.dedupe, AddressNamesDedupe::Name);
        assert_eq!(defaulted.sort, AddressNamesSort::Name);
        assert_eq!(defaulted.order, SortOrder::Asc);

        let params = parse(RawQueryParams {
            relation: Some("owner".to_owned()),
            q: Some(" alice ".to_owned()),
            dedupe: Some("registration".to_owned()),
            sort: Some("expires_at".to_owned()),
            order: Some("desc".to_owned()),
            ..RawQueryParams::default()
        })
        .expect("address-name controls must parse");

        assert_eq!(params.relation, Some(RelationSet::from(Relation::Owner)));
        assert_eq!(params.q, Some("alice".to_owned()));
        assert_eq!(params.dedupe, AddressNamesDedupe::Registration);
        assert_eq!(params.sort, AddressNamesSort::ExpiresAt);
        assert_eq!(params.order, SortOrder::Desc);
    }

    #[test]
    fn relation_sets_parse_any_and_canonicalize_duplicates() {
        let params = parse(RawQueryParams {
            relation: Some("registrant,owner,owner".to_owned()),
            ..RawQueryParams::default()
        })
        .expect("relation set must parse");
        assert_eq!(
            params.relation.as_ref().map(RelationSet::canonical_value),
            Some("owner,registrant".to_owned())
        );

        let params = parse(RawQueryParams {
            relation: Some("any".to_owned()),
            ..RawQueryParams::default()
        })
        .expect("relation any must parse");
        assert_eq!(
            params.relation.as_ref().map(RelationSet::canonical_value),
            Some("owner,manager,registrant".to_owned())
        );

        let error = parse(RawQueryParams {
            relation: Some("any,invalid".to_owned()),
            ..RawQueryParams::default()
        })
        .expect_err("invalid mixed relation set must fail");
        assert_eq!(error.code(), ErrorCode::InvalidInput);
    }

    #[test]
    fn history_scope_defaults_to_both_and_parses_wire_values() {
        let defaulted = parse(RawQueryParams::default()).expect("default query must parse");
        assert_eq!(defaulted.scope, HistoryScope::Both);

        for (wire, expected) in [
            ("name", HistoryScope::Name),
            ("registration", HistoryScope::Registration),
            ("both", HistoryScope::Both),
        ] {
            let params = parse(RawQueryParams {
                scope: Some(wire.to_owned()),
                ..RawQueryParams::default()
            })
            .expect("scope value must parse");
            assert_eq!(params.scope, expected);
        }
    }

    #[test]
    fn event_filters_parse_and_normalize_wire_values() {
        let params = parse(RawQueryParams {
            event_type: Some("registration".to_owned()),
            name: Some(" Alice.eth ".to_owned()),
            registration_id: Some("550e8400-e29b-41d4-a716-446655440000".to_owned()),
            address: Some("0x00000000000000000000000000000000000000aa".to_owned()),
            from_block: Some("10".to_owned()),
            to_block: Some("20".to_owned()),
            ..RawQueryParams::default()
        })
        .expect("event filters must parse");

        assert_eq!(params.event_type, Some(HistoryEventType::Registration));
        assert_eq!(params.name, Some("Alice.eth".to_owned()));
        assert_eq!(
            params.registration_id,
            Some("550e8400-e29b-41d4-a716-446655440000".to_owned())
        );
        assert_eq!(
            params.address,
            Some("0x00000000000000000000000000000000000000aa".to_owned())
        );
        assert_eq!(params.from_block, Some(10));
        assert_eq!(params.to_block, Some(20));
    }

    #[test]
    fn bad_event_filter_values_are_rejected() {
        for raw in [
            RawQueryParams {
                event_type: Some("registered".to_owned()),
                ..RawQueryParams::default()
            },
            RawQueryParams {
                registration_id: Some("not-a-uuid".to_owned()),
                ..RawQueryParams::default()
            },
            RawQueryParams {
                address: Some("0x1234".to_owned()),
                ..RawQueryParams::default()
            },
            RawQueryParams {
                from_block: Some("-1".to_owned()),
                ..RawQueryParams::default()
            },
            RawQueryParams {
                to_block: Some("latest".to_owned()),
                ..RawQueryParams::default()
            },
        ] {
            let error = parse(raw).expect_err("bad event filter must fail");
            assert_eq!(error.code(), ErrorCode::InvalidInput);
        }
    }

    #[test]
    fn at_selector_classifies_rfc3339_timestamp() {
        let params = parse(RawQueryParams {
            at: Some("2026-06-10T00:00:00Z".to_owned()),
            ..RawQueryParams::default()
        })
        .expect("timestamp at selector must parse");

        assert_eq!(
            params.at,
            Some(AtSelector::Timestamp("2026-06-10T00:00:00Z".to_owned()))
        );
    }

    #[test]
    fn at_selector_classifies_opaque_snapshot_token() {
        let params = parse(RawQueryParams {
            at: Some("snapshot_abc-123".to_owned()),
            ..RawQueryParams::default()
        })
        .expect("snapshot token at selector must parse");

        assert_eq!(
            params.at,
            Some(AtSelector::SnapshotToken("snapshot_abc-123".to_owned()))
        );
    }

    #[test]
    fn invalid_at_selector_is_rejected() {
        let error = parse(RawQueryParams {
            at: Some("not a token".to_owned()),
            ..RawQueryParams::default()
        })
        .expect_err("invalid at selector must fail");

        assert_eq!(error.code(), ErrorCode::InvalidInput);
    }
}
